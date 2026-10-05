// Classic script runs before app modules so syntax/import failures are observable.
(function () {
  var socket, id, timer, queue = [], handlers = {}, limit = 60, status = 'Browser diagnostics starting…';
  function safe(value) {
    try { return JSON.parse(JSON.stringify(value, function (_key, item) {
      if (typeof item === 'bigint') return String(item);
      if (item instanceof Error) return { name: item.name, message: item.message, stack: item.stack };
      if (item instanceof Uint8Array) return { type: 'bytes', length: item.length };
      return item;
    })); } catch (_) { return String(value); }
  }
  function display(text) { status = text; var element = document.getElementById('debug-status'); if (element) element.textContent = text; }
  function emit(name, data) {
    var message = JSON.stringify({ kind: 'event', name: name, data: safe(data) });
    if (message.length > 32000) message = JSON.stringify({ kind: 'event', name: name, data: 'Diagnostic output exceeded 32 KiB' });
    if (socket && socket.readyState === 1) socket.send(message);
    else { queue.push(message); if (queue.length > limit) queue.shift(); }
  }
  function snapshot() {
    var state = {};
    ['connection-state', 'status', 'error', 'store-title', 'received', 'cache', 'disk-cache'].forEach(function (name) {
      var node = document.getElementById(name); state[name] = node ? node.textContent : null;
    });
    return { url: location.href, userAgent: navigator.userAgent, platform: navigator.platform, secureContext: window.isSecureContext,
      visibility: document.visibilityState, language: navigator.language, hardwareConcurrency: navigator.hardwareConcurrency,
      deviceMemory: navigator.deviceMemory, viewport: { width: innerWidth, height: innerHeight },
      capabilities: { wasm: typeof WebAssembly !== 'undefined', bigint: typeof BigInt !== 'undefined', worker: typeof Worker !== 'undefined',
        opfs: Boolean(navigator.storage && navigator.storage.getDirectory), crypto: Boolean(window.crypto && crypto.subtle) }, state: state };
  }
  async function probe() {
    var output = snapshot();
    var response = await fetch('/wasm/zsqlite_browser.wasm');
    var bytes = await response.arrayBuffer();
    output.wasm = { status: response.status, bytes: bytes.byteLength, valid: WebAssembly.validate(bytes) };
    var started = performance.now();
    try { await WebAssembly.compile(bytes); output.wasm.compileMs = performance.now() - started; }
    catch (error) { output.wasm.error = safe(error); }
    if (navigator.storage && navigator.storage.estimate) output.storage = await navigator.storage.estimate();
    return output;
  }
  function connect(path) {
    var url = new URL(path, location.href); url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    display('Connecting browser diagnostics…');
    socket = new WebSocket(url.href);
    socket.onopen = function () {
      socket.send(JSON.stringify({ kind: 'hello', info: snapshot() }));
      while (queue.length) socket.send(queue.shift());
      clearInterval(timer); timer = setInterval(function () { emit('heartbeat', snapshot()); }, 10000);
    };
    socket.onmessage = async function (event) {
      try {
        var message = JSON.parse(event.data);
        if (message.kind === 'connected') { id = message.id; display('Live diagnostics connected · ' + id.slice(0, 8)); return; }
        if (message.kind !== 'command') return;
        var result, error;
        try {
          if (message.action === 'snapshot') result = snapshot();
          else if (message.action === 'probe') result = await probe();
          else if (message.action === 'reload') { result = 'Reloading'; setTimeout(function () { location.reload(); }, 100); }
          else if (handlers[message.action]) result = await handlers[message.action](message);
          else throw new Error('App is not ready for ' + message.action);
        } catch (failure) { error = failure.message || String(failure); }
        socket.send(JSON.stringify({ kind: 'result', id: message.id, result: safe(result), error: error }));
      } catch (error) { emit('debug-error', safe(error)); }
    };
    socket.onclose = function () { clearInterval(timer); display('Diagnostics reconnecting…'); setTimeout(function () { connect(path); }, 2000); };
    socket.onerror = function () { display('Diagnostics connection failed'); };
  }
  window.zsqliteDebug = { log: emit, handlers: handlers, snapshot: snapshot };
  window.addEventListener('error', function (event) {
    emit('page-error', { message: event.message, filename: event.filename, line: event.lineno, column: event.colno, error: safe(event.error), resource: event.target && (event.target.src || event.target.href) });
  }, true);
  window.addEventListener('unhandledrejection', function (event) { emit('unhandled-rejection', safe(event.reason)); });
  ['error', 'warn'].forEach(function (name) {
    var original = console[name]; console[name] = function () { emit('console-' + name, Array.prototype.slice.call(arguments).map(safe)); original.apply(console, arguments); };
  });
  window.addEventListener('DOMContentLoaded', function () { emit('dom-ready', snapshot()); display(status); });
  window.addEventListener('pagehide', function () { emit('pagehide', snapshot()); });
  try {
    new PerformanceObserver(function (list) {
      list.getEntries().forEach(function (entry) { emit('resource', { url: entry.name, type: entry.initiatorType, durationMs: entry.duration, transferredBytes: entry.transferSize }); });
    }).observe({ entryTypes: ['resource'] });
  } catch (_) { /* Optional browser feature. */ }
  fetch('/debug/config').then(function (response) { return response.json(); }).then(function (config) {
    if (config.enabled) connect(config.websocket); else display('Browser diagnostics disabled');
  }).catch(function (error) { display('Could not connect diagnostics'); emit('debug-startup-error', safe(error)); });
})();
