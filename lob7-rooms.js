/* LOB7 Rooms SDK. Bundle locally; load before the optional lob7-rooms-p2p.js helper. */
(function () {
  'use strict';
  var PARENT_ORIGINS = ['https://lob7.com', 'https://www.lob7.com'];
  var active = null;
  var connecting = null;
  function identifier() {
    var bytes = new Uint8Array(16);
    globalThis.crypto.getRandomValues(bytes);
    return Array.from(bytes, function (b) { return b.toString(16).padStart(2, '0'); }).join('');
  }
  function errorFrom(value) {
    var error = new Error(value.message || 'Room request failed.');
    error.code = value.code || 'ROOM_REQUEST_FAILED';
    if (Number.isInteger(value.status)) error.status = value.status;
    if (Number.isFinite(value.retryAfterSeconds)) error.retryAfterSeconds = value.retryAfterSeconds;
    return error;
  }
  function validData(value) {
    var todo = [{ value: value, depth: 0 }], seen = new Set(), nodes = 0;
    while (todo.length) {
      var item = todo.pop(), current = item.value;
      if (++nodes > 8192 || item.depth > 64) return false;
      if (typeof current === 'string') { if (current.length > 8192) return false; continue; }
      if (current === null || typeof current === 'boolean') continue;
      if (typeof current === 'number') { if (!Number.isFinite(current)) return false; continue; }
      if (typeof current !== 'object' || seen.has(current)) return false;
      seen.add(current);
      if (!Array.isArray(current) && Object.getPrototypeOf(current) !== Object.prototype && Object.getPrototypeOf(current) !== null) return false;
      var keys = Object.keys(current);
      if (keys.length + todo.length > 8192 || keys.some(function (key) { return key.length > 8192; })) return false;
      if (Array.isArray(current) && (current.length > 8192 || keys.length !== current.length)) return false;
      Object.values(current).forEach(function (child) { todo.push({ value: child, depth: item.depth + 1 }); });
    }
    try { return new TextEncoder().encode(JSON.stringify(value)).byteLength <= 8192; } catch (_) { return false; }
  }
  function connection(room, port) {
    var pending = new Map(), stopped = false, stopSubscription = null, subscriptionError = null, disconnectListeners = new Set();
    function disconnect(reason) {
      if (stopped) return;
      stopped = true;
      disconnectListeners.forEach(function (listener) { listener(errorFrom(reason)); });
      disconnectListeners.clear();
      var onError = subscriptionError;
      if (stopSubscription) stopSubscription();
      if (onError) { try { onError(errorFrom(reason)); } catch (_) { /* observer only */ } }
      pending.forEach(function (task) { globalThis.clearTimeout(task.timer); task.reject(errorFrom(reason)); });
      pending.clear();
      port.close();
      if (active === result) active = null;
    }
    port.onmessage = function (event) {
      var response = event.data;
      if (response && response.type === 'disconnected') return disconnect(response.error || { code: 'ROOM_CHANGED' });
      var task = response && pending.get(response.requestId);
      if (!task) return;
      globalThis.clearTimeout(task.timer);
      pending.delete(response.requestId);
      if (response.error) task.reject(errorFrom(response.error));
      else task.resolve(response.result);
    };
    port.onmessageerror = function () { disconnect({ code: 'ROOM_CONNECTION_CLOSED', message: 'The room connection closed. Connect again.' }); };
    port.start();
    function request(method, params) {
      if (stopped) return Promise.reject(errorFrom({ code: 'ROOM_CONNECTION_CLOSED', message: 'Connect to the selected room again.' }));
      if (pending.size >= 4) return Promise.reject(errorFrom({ code: 'ROOM_BUSY', message: 'Too many requests are in progress.', retryAfterSeconds: 3 }));
      var id = identifier();
      return new Promise(function (resolve, reject) {
        var timer = globalThis.setTimeout(function () {
          pending.delete(id);
          reject(errorFrom({ code: 'ROOM_TIMEOUT', message: 'The room request timed out. Retry a send using the same idempotency key.' }));
        }, 25000);
        pending.set(id, { resolve: resolve, reject: reject, timer: timer });
        try { port.postMessage({ requestId: id, method: method, params: params }); }
        catch (_) { globalThis.clearTimeout(timer); pending.delete(id); reject(errorFrom({ code: 'ROOM_PAYLOAD_INVALID' })); }
      });
    }
    var result = {
      room: room,
      read: function (options) {
        options = options || {};
        return request('read', { after: options.after === undefined ? 0 : options.after, limit: options.limit === undefined ? 50 : options.limit }).then(function (batch) {
          if (Number.isSafeInteger(batch.cursor)) room.lastSequence = Math.max(room.lastSequence || 0, batch.cursor);
          return batch;
        });
      },
      send: function (data, options) {
        var key = options && options.idempotencyKey || identifier();
        var promise = validData(data)
          ? request('send', { data: data, idempotencyKey: key })
          : Promise.reject(errorFrom({ code: 'ROOM_PAYLOAD_INVALID', message: 'Use finite JSON data no larger than 8192 UTF-8 bytes.' }));
        return promise.then(function (response) { return response.message; }).catch(function (error) { error.idempotencyKey = key; throw error; });
      },
      subscribe: function (callback, options) {
        options = options || {};
        if (typeof callback !== 'function') throw new TypeError('subscribe requires a callback.');
        if (stopSubscription) throw errorFrom({ code: 'ROOM_SUBSCRIPTION_EXISTS', message: 'Stop the current subscription before starting another.' });
        if (stopped) throw errorFrom({ code: 'ROOM_CONNECTION_CLOSED', message: 'Connect to the selected room again.' });
        var cursor = options.after === undefined ? 0 : options.after;
        if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TypeError('after must be a nonnegative integer.');
        var cancelled = false, timer = null, delay = 3000;
        function stop() { cancelled = true; if (timer !== null) globalThis.clearTimeout(timer); if (stopSubscription === stop) { stopSubscription = null; subscriptionError = null; } }
        async function poll() {
          if (cancelled || stopped) return;
          try {
            var batch = await result.read({ after: cursor });
            for (var message of batch.messages) {
              if (cancelled || stopped) return;
              await callback(message);
              cursor = message.sequence;
            }
            cursor = Math.max(cursor, batch.cursor);
            delay = batch.messages.length ? 3000 : Math.min(delay * 1.5, 15000);
            delay = Math.max(delay, (batch.pollAfterSeconds || 3) * 1000);
          } catch (error) {
            if (cancelled || stopped) return;
            if (['ROOM_EXPIRED', 'ROOM_NOT_FOUND', 'RELEASE_NOT_FOUND', 'ACCOUNT_SUSPENDED', 'GAME_NOT_ALLOWED', 'SCOPE_REQUIRED', 'AUTH_INVALID', 'AUTH_REQUIRED'].includes(error.code)) {
              disconnect(error);
              return;
            }
            delay = Math.max(Math.min(delay * 2, 30000), (error.retryAfterSeconds || 0) * 1000);
            if (typeof options.onError === 'function') { try { options.onError(error); } catch (_) { /* callback cannot break cleanup */ } }
          }
          if (!cancelled && !stopped) timer = globalThis.setTimeout(poll, delay + Math.floor(Math.random() * 250));
        }
        stopSubscription = stop;
        subscriptionError = typeof options.onError === 'function' ? options.onError : null;
        void poll();
        return stop;
      },
    };
    if (typeof globalThis.LOB7RoomsP2P === 'function') result.connectPeer = function (options) {
      return globalThis.LOB7RoomsP2P(result, options, function (listener) {
        if (stopped) throw errorFrom({ code: 'ROOM_CONNECTION_CLOSED', message: 'Connect to the selected room again.' });
        disconnectListeners.add(listener);
        return function () { disconnectListeners.delete(listener); };
      }, { identifier: identifier, errorFrom: errorFrom, validData: validData });
    };
    globalThis.addEventListener('pagehide', function () { disconnect({ code: 'ROOM_CONNECTION_CLOSED', message: 'The game document closed.' }); }, { once: true });
    return result;
  }
  function connect() {
    if (active) return Promise.resolve(active);
    if (connecting) return connecting;
    if (globalThis.parent === globalThis) return Promise.reject(errorFrom({ code: 'ROOM_PREVIEW_REQUIRED', message: 'Open this project in its LOB7 preview and choose a room.' }));
    connecting = new Promise(function (resolve, reject) {
      var nonce = identifier(), requestId = identifier(), timeout, deferred;
      function cleanup() {
        globalThis.removeEventListener('message', receive);
        globalThis.removeEventListener('load', ready);
        globalThis.clearTimeout(timeout);
        if (deferred !== undefined) globalThis.clearTimeout(deferred);
      }
      function receive(event) {
        var response = event.data;
        if (event.source !== globalThis.parent || !PARENT_ORIGINS.includes(event.origin) || !response || response.type !== 'lob7.rooms.connected'
          || response.nonce !== nonce || response.requestId !== requestId) return;
        cleanup();
        if (response.error) return reject(errorFrom(response.error));
        if (!response.room || !event.ports[0]) return reject(errorFrom({ code: 'ROOM_PROTOCOL_INVALID' }));
        active = connection(response.room, event.ports[0]);
        resolve(active);
      }
      // Run after the parent's iframe load handler has invalidated any previous document.
      function ready() { deferred = globalThis.setTimeout(function () { PARENT_ORIGINS.forEach(function (origin) { globalThis.parent.postMessage({ type: 'lob7.rooms.connect', nonce: nonce, requestId: requestId }, origin); }); }, 0); }
      globalThis.addEventListener('message', receive);
      timeout = globalThis.setTimeout(function () { cleanup(); reject(errorFrom({ code: 'ROOM_BRIDGE_UNAVAILABLE', message: 'This document is not connected to the LOB7 room panel.' })); }, 8000);
      if (globalThis.document.readyState === 'complete') ready();
      else globalThis.addEventListener('load', ready, { once: true });
    }).finally(function () { connecting = null; });
    return connecting;
  }
  globalThis.LOB7Rooms = Object.freeze({ connect: connect });
})();
