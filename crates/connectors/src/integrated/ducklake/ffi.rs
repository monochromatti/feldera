//! A hand-declared subset of the DuckDB C API, resolved at run time.
//!
//! Why not a crate: the `duckdb` crate tracks DuckDB releases and its `bundled`
//! feature compiles DuckDB from source, which would put a multi-minute C++
//! build in front of every `cargo build` of this workspace and pin us to
//! whatever version that crate last packaged.  The DuckLake extension requires
//! an exact DuckDB version anyway (it is a signed binary built against one), so
//! the library has to be *chosen*, not compiled in: the deployment provides
//! `libduckdb` of the matching version and this module loads it.
//!
//! Why not `libloading`: the workspace already depends on `libc`, and the
//! subset below is small enough that `dlopen`/`dlsym` is less code than a new
//! dependency.
//!
//! The declarations mirror `duckdb.h` from DuckDB v2.0.0.  Only what the
//! connector uses is declared; anything else is a compile error rather than a
//! silently-wrong signature, and every function is looked up eagerly at load
//! time so a version mismatch fails loudly once instead of halfway through a
//! publication.

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;

use anyhow::{Context, Result, anyhow, bail};

pub struct Library {
    /// The `dlopen` handle.  Never closed: the library stays loaded for the
    /// life of the process, because a `Database` may outlive the `Library` value
    /// that created it and unloading would leave it calling into freed pages.
    #[allow(dead_code)]
    handle: *mut c_void,
    fns: Fns,
}

// SAFETY: the handle is only read (never written) after `load`, and every
// method either takes `&self` for pure symbol calls or operates on values the
// caller owns.  DuckDB is thread-safe for separate connections, which is how
// the connector uses it.
unsafe impl Send for Library {}
unsafe impl Sync for Library {}

// SAFETY: a database and a connection are only used by the worker that opened
// them; the connector never shares one across threads (each DuckDB connection is
// its own transaction context).  `Config` is consumed by `open`.
unsafe impl Send for Database {}
unsafe impl Send for Connection {}
unsafe impl Send for Config {}

#[allow(clippy::type_complexity)]
struct Fns {
    create_config: unsafe extern "C" fn(*mut Config) -> State,
    set_config: unsafe extern "C" fn(Config, *const c_char, *const c_char) -> State,
    destroy_config: unsafe extern "C" fn(*mut Config),
    open_ext: unsafe extern "C" fn(*const c_char, *mut Database, Config, *mut *mut c_char) -> State,
    connect: unsafe extern "C" fn(Database, *mut Connection) -> State,
    close: unsafe extern "C" fn(*mut Database),
    disconnect: unsafe extern "C" fn(*mut Connection),
    query: unsafe extern "C" fn(Connection, *const c_char, *mut Result_) -> State,
    destroy_result: unsafe extern "C" fn(*mut Result_),
    result_error: unsafe extern "C" fn(*mut Result_) -> *const c_char,
    column_count: unsafe extern "C" fn(*mut Result_) -> u64,
    row_count: unsafe extern "C" fn(*mut Result_) -> u64,
    value_varchar: unsafe extern "C" fn(*mut Result_, u64, u64) -> *mut c_char,
    value_is_null: unsafe extern "C" fn(*mut Result_, u64, u64) -> bool,
    prepare: unsafe extern "C" fn(Connection, *const c_char, *mut PreparedStatement) -> State,
    prepare_error: unsafe extern "C" fn(PreparedStatement) -> *const c_char,
    destroy_prepare: unsafe extern "C" fn(*mut PreparedStatement),
    bind_null: unsafe extern "C" fn(PreparedStatement, u64) -> State,
    bind_boolean: unsafe extern "C" fn(PreparedStatement, u64, bool) -> State,
    bind_int64: unsafe extern "C" fn(PreparedStatement, u64, i64) -> State,
    bind_double: unsafe extern "C" fn(PreparedStatement, u64, f64) -> State,
    bind_varchar: unsafe extern "C" fn(PreparedStatement, u64, *const c_char) -> State,
    execute_prepared: unsafe extern "C" fn(PreparedStatement, *mut Result_) -> State,
    free: unsafe extern "C" fn(*mut c_void),
    library_version: unsafe extern "C" fn() -> *const c_char,
}

pub type State = u32;
pub const SUCCESS: State = 0;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Database {
    internal_ptr: *mut c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Connection {
    ptr: *mut c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Config {
    ptr: *mut c_void,
}

/// The owner of a `duckdb_config`: a `Config` is a copyable handle, this type
/// destroys it.
///
/// DuckDB's contract is that the caller destroys the config it created, *after*
/// using it to open (or not) — not that `duckdb_open_ext` consumes it.  The
/// handle was leaked on every connector instantiation, including on the error
/// paths that never reach `open_ext`.
pub struct Owned<'a> {
    lib: &'a Library,
    config: Config,
}

impl std::ops::Deref for Owned<'_> {
    type Target = Config;

    fn deref(&self) -> &Config {
        &self.config
    }
}

impl Drop for Owned<'_> {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `duckdb_create_config`, this is the only
        // `Drop` for it, and the library outlives the config (borrowed).
        unsafe { (self.lib.fns.destroy_config)(&mut self.config) };
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct PreparedStatement {
    ptr: *mut c_void,
}

#[repr(C)]
pub struct Result_ {
    deprecated_column_count: u64,
    deprecated_row_count: u64,
    deprecated_rows_changed: u64,
    deprecated_columns: *mut c_void,
    deprecated_error_message: *mut c_char,
    internal_data: *mut c_void,
}

impl Library {
    /// Load the DuckDB shared library and resolve every symbol it declares.
    ///
    /// The search order is: `$DUCKLAKE_DUCKDB_LIB`, then the platform's usual
    /// names through the dynamic loader.  An explicit path wins because the
    /// extension binary and the library must come from the same release; a
    /// silently-picked system `libduckdb` is a version mismatch waiting to
    /// happen.
    pub fn load() -> Result<&'static Library> {
        static LIB: std::sync::OnceLock<std::result::Result<Library, String>> =
            std::sync::OnceLock::new();
        let loaded = LIB.get_or_init(|| Self::load_uncached().map_err(|e| format!("{e:#}")));
        match loaded {
            Ok(lib) => Ok(lib),
            Err(e) => Err(anyhow!("{e}")),
        }
    }

    fn load_uncached() -> Result<Library> {
        let candidates: Vec<String> = match std::env::var("DUCKLAKE_DUCKDB_LIB") {
            Ok(path) if !path.is_empty() => vec![path],
            _ => vec![
                "libduckdb.dylib".to_owned(),
                "libduckdb.so".to_owned(),
                "libduckdb.2.dylib".to_owned(),
                "libduckdb.so.2".to_owned(),
            ],
        };

        let mut last_err = None;
        for candidate in &candidates {
            let cpath = CString::new(candidate.as_str())?;
            // SAFETY: the path is a valid C string and the flags are constant.
            let handle =
                unsafe { dlopen(cpath.as_ptr(), dlflags::RTLD_LAZY | dlflags::RTLD_LOCAL) };
            if handle.is_null() {
                last_err = Some(format!("{candidate}: {}", dlerror_message()));
                continue;
            }
            match unsafe { Fns::resolve(handle) } {
                Ok(fns) => {
                    let lib = Library { handle, fns };
                    tracing::info!(
                        "ducklake: loaded libduckdb {candidate} version {}",
                        lib.version()
                    );
                    return Ok(lib);
                }
                Err(e) => {
                    // SAFETY: `handle` came from `dlopen` and is not used again.
                    unsafe { dlclose(handle) };
                    last_err = Some(format!("{candidate}: {e}"));
                }
            }
        }
        bail!(
            "cannot load libduckdb (set DUCKLAKE_DUCKDB_LIB to the pinned library): {}",
            last_err.unwrap_or_else(|| "no candidates".to_owned())
        )
    }

    pub fn version(&self) -> String {
        // SAFETY: the symbol is resolved and DuckDB returns a static string.
        unsafe { CStr::from_ptr((self.fns.library_version)()) }
            .to_string_lossy()
            .into_owned()
    }

    /// A fresh config, destroyed when the returned owner is dropped.
    pub fn config(&self) -> Result<Owned<'_>> {
        let mut config = Config {
            ptr: ptr::null_mut(),
        };
        // SAFETY: `config` is a valid out-pointer.
        check(unsafe { (self.fns.create_config)(&mut config) })?;
        Ok(Owned { lib: self, config })
    }

    pub fn set_config(&self, config: Config, name: &str, value: &str) -> Result<()> {
        let name = CString::new(name)?;
        let value = CString::new(value)?;
        // SAFETY: both strings outlive the call and `config` came from `config`.
        check(unsafe { (self.fns.set_config)(config, name.as_ptr(), value.as_ptr()) })
            .with_context(|| format!("setting DuckDB option {name:?}"))
    }

    /// Point DuckDB at the directory to find (and install) extensions in.
    ///
    /// DuckDB 2.0 deprecated `extension_directory` in favour of
    /// `extension_directories`, which is a *list*: the singular key still works
    /// but warns, and is gone from `duckdb_settings()` altogether, so it will be
    /// removed.  Both take a string through the C API, but the list setting
    /// refuses a bare path — measured: `duckdb_set_config(config,
    /// "extension_directories", "/some/dir")` returns an error, the open then
    /// proceeds *without* the setting, and `LOAD ducklake` goes to the default
    /// directory or the network.  A silent misconfiguration is the worst outcome
    /// for an offline deployment, so the list form is written here once and the
    /// deprecated key is only a fallback: no version sniffing, and a DuckDB that
    /// drops the old key still gets the right directory.
    pub fn set_extension_directory(&self, config: Config, directory: &str) -> Result<()> {
        // Quoted, so a comma or a space in the path is not read as a separator.
        let list = format!("['{}']", directory.replace('\'', "''"));
        // Keep the first failure: if both keys are refused, the reason the current
        // one was refused is the diagnostic, not the fact that there were two.
        let list_error = match self.set_config(config, "extension_directories", &list) {
            Ok(()) => return Ok(()),
            Err(e) => format!("{e:#}"),
        };
        self.set_config(config, "extension_directory", directory)
            .with_context(|| {
                format!(
                    "setting neither extension_directories ({list_error}) nor \
                     extension_directory to {directory:?}"
                )
            })
    }

    pub fn open(&self, path: &str, config: Config) -> Result<Database> {
        let path = CString::new(path)?;
        let mut db = Database {
            internal_ptr: ptr::null_mut(),
        };
        let mut err: *mut c_char = ptr::null_mut();
        // SAFETY: `db` and `err` are valid out-pointers; DuckDB copies the config
        // during the call, and the caller destroys it afterwards (`Owned`).
        let state = unsafe { (self.fns.open_ext)(path.as_ptr(), &mut db, config, &mut err) };
        if state != SUCCESS {
            let message = take_c_string(self, err);
            bail!("opening DuckDB database {path:?} failed: {message}");
        }
        Ok(db)
    }

    pub fn connect(&self, db: Database) -> Result<Conn<'_>> {
        let mut conn = Connection {
            ptr: ptr::null_mut(),
        };
        // SAFETY: `db` is open for the lifetime of `self`.
        check(unsafe { (self.fns.connect)(db, &mut conn) })?;
        Ok(Conn {
            lib: self,
            conn,
            open: true,
        })
    }

    /// Run a statement with no result rows (DDL, `BEGIN`, `COMMIT`, ...).
    pub fn execute(&self, conn: Connection, sql: &str, params: &[Value]) -> Result<()> {
        self.run(conn, sql, params).map(|_| ())
    }

    /// Run a statement and collect its rows as text.
    ///
    /// Values come back as `varchar` because the connector only reads guard
    /// columns and counters here; rendering typed values into Rust types would
    /// need a mapping the connector does not otherwise use.
    pub fn run(&self, conn: Connection, sql: &str, params: &[Value]) -> Result<Rows> {
        if params.is_empty() {
            return self.run_simple(conn, sql);
        }
        let sql_c = CString::new(sql)?;
        let mut stmt = PreparedStatement {
            ptr: ptr::null_mut(),
        };
        // SAFETY: `stmt` is a valid out-pointer.
        let state = unsafe { (self.fns.prepare)(conn, sql_c.as_ptr(), &mut stmt) };
        if state != SUCCESS {
            // SAFETY: a failed prepare leaves `stmt` usable for the error text,
            // but the text itself may be absent — reading a null pointer here
            // would be a straight dereference of address 0.
            let text = unsafe { (self.fns.prepare_error)(stmt) };
            let message = if text.is_null() {
                "no error text".to_owned()
            } else {
                unsafe { CStr::from_ptr(text) }
                    .to_string_lossy()
                    .into_owned()
            };
            // SAFETY: the statement is not executed and is destroyed once.
            unsafe { (self.fns.destroy_prepare)(&mut stmt) };
            bail!("preparing {sql:?} failed: {message}");
        }

        let guard = StmtGuard { lib: self, stmt };
        for (i, param) in params.iter().enumerate() {
            // DuckDB parameters are 1-indexed.
            let idx = i as u64 + 1;
            let stmt = guard.stmt;
            // SAFETY: the statement is alive; each bind copies the value.
            let state = unsafe {
                match param {
                    Value::Null => (self.fns.bind_null)(stmt, idx),
                    Value::Bool(b) => (self.fns.bind_boolean)(stmt, idx, *b),
                    Value::Int(i) => (self.fns.bind_int64)(stmt, idx, *i),
                    Value::Float(f) => (self.fns.bind_double)(stmt, idx, *f),
                    Value::Text(s) => {
                        let cs = CString::new(s.as_str()).with_context(|| {
                            format!("parameter {idx} of {sql:?} contains a NUL byte")
                        })?;
                        (self.fns.bind_varchar)(stmt, idx, cs.as_ptr())
                    }
                }
            };
            check(state).with_context(|| format!("binding parameter {idx} of {sql:?}"))?;
        }

        let mut res: Result_ = unsafe { std::mem::zeroed() };
        // SAFETY: the statement is alive and prepared with `guard.stmt`.
        let state = unsafe { (self.fns.execute_prepared)(guard.stmt, &mut res) };
        collect(self, res, state, sql)
    }

    fn run_simple(&self, conn: Connection, sql: &str) -> Result<Rows> {
        let sql_c = CString::new(sql)?;
        let mut res: Result_ = unsafe { std::mem::zeroed() };
        // SAFETY: `res` is a valid out-pointer for the duration of the call.
        let state = unsafe { (self.fns.query)(conn, sql_c.as_ptr(), &mut res) };
        collect(self, res, state, sql)
    }
}

fn collect(lib: &Library, mut res: Result_, state: State, sql: &str) -> Result<Rows> {
    if state != SUCCESS {
        // SAFETY: the result is alive and readable after a failed execution.
        let p = unsafe { (lib.fns.result_error)(&mut res) };
        let message = if p.is_null() {
            String::new()
        } else {
            // SAFETY: DuckDB owns the error text; it is borrowed, not freed.
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
        };
        // SAFETY: the result is destroyed exactly once.
        unsafe { (lib.fns.destroy_result)(&mut res) };
        bail!("{sql:?} failed: {message}");
    }
    // SAFETY: the result is alive until it is destroyed below.
    let (cols, count) = unsafe {
        (
            (lib.fns.column_count)(&mut res),
            (lib.fns.row_count)(&mut res),
        )
    };
    let mut rows = Vec::with_capacity(count as usize);
    for r in 0..count {
        let mut row = Vec::with_capacity(cols as usize);
        for c in 0..cols {
            // SAFETY: `(c, r)` is inside the result's dimensions.
            let cell = unsafe {
                if (lib.fns.value_is_null)(&mut res, c, r) {
                    None
                } else {
                    Some(take_c_string(lib, (lib.fns.value_varchar)(&mut res, c, r)))
                }
            };
            row.push(cell);
        }
        rows.push(row);
    }
    // SAFETY: the result is destroyed exactly once, after the rows are copied.
    unsafe { (lib.fns.destroy_result)(&mut res) };
    Ok(Rows { rows })
}

fn check(state: State) -> Result<()> {
    if state == SUCCESS {
        Ok(())
    } else {
        bail!("DuckDB returned error code {state}")
    }
}

fn take_c_string(lib: &Library, p: *mut c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: DuckDB hands out NUL-terminated strings it expects us to free.
    let text = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    // SAFETY: the pointer came from DuckDB and is freed exactly once.
    unsafe { (lib.fns.free)(p as *mut c_void) };
    text
}

fn dlerror_message() -> String {
    // SAFETY: `dlerror` returns either null or a NUL-terminated static string,
    // and clears the error state as a side effect.
    unsafe {
        let err = dlerror();
        if err.is_null() {
            "unknown dlopen error".to_owned()
        } else {
            CStr::from_ptr(err).to_string_lossy().into_owned()
        }
    }
}

/// `dlopen` flags, which are **not** the same on every platform: `RTLD_LOCAL`
/// is `0x4` on macOS but `0` on glibc and musl, where `0x4` is `RTLD_NOLOAD`
/// ("only if already resident").  A single hardcoded `0x4` therefore made every
/// `dlopen` of a not-yet-loaded libduckdb fail on Linux with a message that
/// blamed the path, so the flags are per-platform.  `libc` would supply them,
/// but it would be a dependency for two integers.
#[cfg(target_os = "macos")]
mod dlflags {
    pub const RTLD_LAZY: i32 = 0x1;
    pub const RTLD_LOCAL: i32 = 0x4;
}
#[cfg(all(unix, not(target_os = "macos")))]
mod dlflags {
    pub const RTLD_LAZY: i32 = 0x1;
    pub const RTLD_LOCAL: i32 = 0x0;
}

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flag: i32) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> i32;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlerror() -> *const c_char;
}

impl Fns {
    /// # Safety
    ///
    /// `handle` must be a live handle from `dlopen`, and every symbol resolved
    /// here must exist in the library it refers to with the declared signature.
    unsafe fn resolve(handle: *mut c_void) -> Result<Fns> {
        macro_rules! sym {
            ($name:literal : $ty:ty) => {{
                let name = CString::new($name)?;
                // SAFETY: the caller guarantees `handle` is a valid handle.
                let p = unsafe { dlsym(handle, name.as_ptr()) };
                if p.is_null() {
                    bail!("libduckdb is missing symbol {}", $name);
                }
                // SAFETY: the symbol exists; the signature is the one declared
                // in `duckdb.h` for this DuckDB version.
                unsafe { std::mem::transmute::<*mut c_void, $ty>(p) }
            }};
        }
        Ok(Fns {
            create_config: sym!("duckdb_create_config": unsafe extern "C" fn(*mut Config) -> State),
            set_config: sym!("duckdb_set_config": unsafe extern "C" fn(Config, *const c_char, *const c_char) -> State),
            destroy_config: sym!("duckdb_destroy_config": unsafe extern "C" fn(*mut Config)),
            open_ext: sym!("duckdb_open_ext": unsafe extern "C" fn(*const c_char, *mut Database, Config, *mut *mut c_char) -> State),
            connect: sym!("duckdb_connect": unsafe extern "C" fn(Database, *mut Connection) -> State),
            close: sym!("duckdb_close": unsafe extern "C" fn(*mut Database)),
            disconnect: sym!("duckdb_disconnect": unsafe extern "C" fn(*mut Connection)),
            query: sym!("duckdb_query": unsafe extern "C" fn(Connection, *const c_char, *mut Result_) -> State),
            destroy_result: sym!("duckdb_destroy_result": unsafe extern "C" fn(*mut Result_)),
            result_error: sym!("duckdb_result_error": unsafe extern "C" fn(*mut Result_) -> *const c_char),
            column_count: sym!("duckdb_column_count": unsafe extern "C" fn(*mut Result_) -> u64),
            row_count: sym!("duckdb_row_count": unsafe extern "C" fn(*mut Result_) -> u64),
            value_varchar: sym!("duckdb_value_varchar": unsafe extern "C" fn(*mut Result_, u64, u64) -> *mut c_char),
            value_is_null: sym!("duckdb_value_is_null": unsafe extern "C" fn(*mut Result_, u64, u64) -> bool),
            prepare: sym!("duckdb_prepare": unsafe extern "C" fn(Connection, *const c_char, *mut PreparedStatement) -> State),
            prepare_error: sym!("duckdb_prepare_error": unsafe extern "C" fn(PreparedStatement) -> *const c_char),
            destroy_prepare: sym!("duckdb_destroy_prepare": unsafe extern "C" fn(*mut PreparedStatement)),
            bind_null: sym!("duckdb_bind_null": unsafe extern "C" fn(PreparedStatement, u64) -> State),
            bind_boolean: sym!("duckdb_bind_boolean": unsafe extern "C" fn(PreparedStatement, u64, bool) -> State),
            bind_int64: sym!("duckdb_bind_int64": unsafe extern "C" fn(PreparedStatement, u64, i64) -> State),
            bind_double: sym!("duckdb_bind_double": unsafe extern "C" fn(PreparedStatement, u64, f64) -> State),
            bind_varchar: sym!("duckdb_bind_varchar": unsafe extern "C" fn(PreparedStatement, u64, *const c_char) -> State),
            execute_prepared: sym!("duckdb_execute_prepared": unsafe extern "C" fn(PreparedStatement, *mut Result_) -> State),
            free: sym!("duckdb_free": unsafe extern "C" fn(*mut c_void)),
            library_version: sym!("duckdb_library_version": unsafe extern "C" fn() -> *const c_char),
        })
    }
}

/// A statement parameter, mirroring the JSON types the connector receives.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Value {
    /// Convert a JSON value to a bindable parameter.
    ///
    /// Numbers that are not exactly representable as `i64` become `f64`, and
    /// anything structured becomes its JSON text: DuckDB casts a text parameter
    /// to the column's type, which covers timestamps and decimals arriving as
    /// strings.  A structured value bound to a `LIST`/`STRUCT` column needs a
    /// real conversion and is rejected by DuckDB rather than guessed at here.
    pub fn from_json(value: &serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => match n.as_f64() {
                    Some(f) => Value::Float(f),
                    None => Value::Text(n.to_string()),
                },
            },
            serde_json::Value::String(s) => Value::Text(s.clone()),
            other => Value::Text(other.to_string()),
        }
    }
}

pub struct Rows {
    rows: Vec<Vec<Option<String>>>,
}

impl Rows {
    pub fn into_rows(self) -> Vec<Vec<Option<String>>> {
        self.rows
    }
}

struct StmtGuard<'a> {
    lib: &'a Library,
    stmt: PreparedStatement,
}

impl Drop for StmtGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the statement was prepared by this library and is destroyed once.
        unsafe { (self.lib.fns.destroy_prepare)(&mut self.stmt) };
    }
}

/// A DuckDB connection, closed on drop.
pub struct Conn<'a> {
    lib: &'a Library,
    conn: Connection,
    open: bool,
}

impl Conn<'_> {
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<()> {
        self.lib.execute(self.conn, sql, params)
    }

    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Rows> {
        self.lib.run(self.conn, sql, params)
    }

    pub fn close(&mut self) {
        if self.open {
            // SAFETY: the connection came from `connect` and is closed once.
            unsafe { (self.lib.fns.disconnect)(&mut self.conn) };
            self.open = false;
        }
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

/// An open DuckDB database, closed on drop.
pub struct Db<'a> {
    lib: &'a Library,
    db: Database,
}

impl<'a> Db<'a> {
    pub fn open(lib: &'a Library, path: &str, config: Config) -> Result<Db<'a>> {
        Ok(Db {
            lib,
            db: lib.open(path, config)?,
        })
    }

    pub fn connect(&self) -> Result<Conn<'a>> {
        self.lib.connect(self.db)
    }
}

impl Drop for Db<'_> {
    fn drop(&mut self) {
        // SAFETY: the database is open and is closed once.
        unsafe { (self.lib.fns.close)(&mut self.db) };
    }
}
