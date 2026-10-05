#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v emcc >/dev/null || { echo 'Activate emsdk before building (source /path/to/emsdk/emsdk_env.sh)' >&2; exit 1; }
exports='["_main","_malloc","_free","_zsqlite_browser_open","_zsqlite_browser_error","_sqlite3_close","_sqlite3_db_status","_sqlite3_errmsg","_sqlite3_prepare_v2","_sqlite3_stmt_readonly","_sqlite3_step","_sqlite3_finalize","_sqlite3_reset","_sqlite3_clear_bindings","_sqlite3_bind_parameter_count","_sqlite3_bind_parameter_name","_sqlite3_bind_null","_sqlite3_bind_int64","_sqlite3_bind_double","_sqlite3_bind_text","_sqlite3_bind_blob","_sqlite3_column_count","_sqlite3_column_name","_sqlite3_column_type","_sqlite3_column_int64","_sqlite3_column_double","_sqlite3_column_text","_sqlite3_column_blob","_sqlite3_column_bytes"]'
export CFLAGS_wasm32_unknown_emscripten='-DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION'
# The package also produces a cdylib; its standalone WASM has no main function.
export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=--no-entry"
cargo rustc --release --target wasm32-unknown-emscripten --no-default-features --features browser --bin zsqlite-browser -- \
  -C link-arg=-sMODULARIZE=1 -C link-arg=-sEXPORT_ES6=1 -C link-arg=-sEXPORT_NAME=createZsqlite \
  -C link-arg=-sENVIRONMENT=worker -C link-arg=-sALLOW_MEMORY_GROWTH=1 -C link-arg=-sWASM_BIGINT=1 \
  -C link-arg=-sASYNCIFY=1 -C link-arg=-sASYNCIFY_STACK_SIZE=1048576 \
  -C 'link-arg=-sASYNCIFY_IMPORTS=["zsqlite_http_read","zsqlite_http_read_many","zsqlite_http_stat"]' \
  -C link-arg=-sSTACK_SIZE=1048576 -C link-arg=-sINITIAL_MEMORY=33554432 \
  -C "link-arg=-sEXPORTED_FUNCTIONS=$exports" \
  -C 'link-arg=-sEXPORTED_RUNTIME_METHODS=["ccall","UTF8ToString","stringToUTF8","lengthBytesUTF8","HEAPU8","HEAPU32"]' \
  -C link-arg=--js-library -C "link-arg=$PWD/web/bridge.js"
npm --prefix web run build:typescript
mkdir -p web/dist
browser_target="${CARGO_TARGET_DIR:-target}/wasm32-unknown-emscripten/release"
cp "$browser_target/zsqlite-browser.js" web/dist/zsqlite-browser.mjs
cp "$browser_target/zsqlite_browser.wasm" web/dist/
echo 'Browser package built in web/dist/'
