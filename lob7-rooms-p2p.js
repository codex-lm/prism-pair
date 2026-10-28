/* Optional LOB7 Rooms P2P helper. Load after lob7-rooms.js, only in p2p games. */
(function () {
  'use strict';
  /** Optional two-player transport. Game rules and state remain entirely in the game. */
  globalThis.LOB7RoomsP2P = function (lobby, options, onDisconnect, helpers) {
    var identifier = helpers.identifier, errorFrom = helpers.errorFrom, validData = helpers.validData;
    options = options || {};
    var room = lobby.room, me = room.me && room.me.id, host = room.hostId;
    if (room.mode !== 'p2p' || room.maxPlayers !== 2 || !me || !host) return Promise.reject(errorFrom({ code: 'PEER_ROOM_REQUIRED', message: 'Choose a two-player direct room in the LOB7 panel first.' }));
    if (typeof globalThis.RTCPeerConnection !== 'function') return Promise.reject(errorFrom({ code: 'PEER_UNAVAILABLE', message: 'This browser does not support direct play.' }));
    var role = room.me.role === 'owner' ? 'host' : 'guest';
    var session = role === 'guest' ? identifier() : null, remote = role === 'guest' ? host : null;
    // A fresh host challenge binds both sides to this attempt, not room history.
    var hostSession = role === 'host' ? identifier() : null;
    var acknowledgedHosts = new Set();
    var pc = null, dataChannel = null, done = false, opened = false, offered = false;
    var stop = null, removeDisconnect = null, helloTimer = null, disconnectedTimer = null, deadline = null;
    var gatherTimer = null, gathered = null;
    var candidates = [], candidateCount = 0, queue = Promise.resolve(), resolvePeer, rejectPeer;
    var timeout = Number.isFinite(options.timeoutMs) ? Math.max(5000, Math.min(180000, options.timeoutMs)) : 90000;
    var signal = options.signal;
    var failureText = 'Direct connection is unavailable on this network. Try another network or return to solo play.';
    var promise = new Promise(function (resolve, reject) { resolvePeer = resolve; rejectPeer = reject; });
    function state(value) { if (typeof options.onState === 'function') { try { options.onState(value); } catch (_) { /* observer only */ } } }
    function finish(error) {
      if (done) return;
      done = true;
      globalThis.clearTimeout(deadline); globalThis.clearTimeout(helloTimer); globalThis.clearTimeout(disconnectedTimer);
      if (gathered) { gathered(error); gathered = null; }
      globalThis.clearTimeout(gatherTimer);
      if (stop) stop();
      if (removeDisconnect) removeDisconnect();
      if (signal) signal.removeEventListener('abort', cancel);
      if (dataChannel) { dataChannel.onopen = dataChannel.onmessage = dataChannel.onerror = dataChannel.onclose = null; dataChannel.close(); }
      if (pc) { pc.onicecandidate = pc.onicegatheringstatechange = pc.onconnectionstatechange = pc.ondatachannel = null; pc.close(); }
      if (!opened) rejectPeer(error);
      else if (typeof options.onClose === 'function') { try { options.onClose(error); } catch (_) { /* observer only */ } }
      state('closed');
    }
    function fail(error) { finish(error && error.code ? error : errorFrom({ code: 'PEER_CONNECTION_FAILED', message: failureText })); }
    function cancel() { finish(errorFrom({ code: 'PEER_CLOSED', message: 'You cancelled the direct connection.' })); }
    function sendSignal(kind, value) {
      var packet = { protocol: 'lob7.peer.v1', kind: kind, session: kind === 'ready' ? hostSession : session, hostSession: hostSession, to: remote, value: value === undefined ? null : value };
      if (!validData(packet)) {
        var tooLarge = errorFrom({ code: 'PEER_SIGNAL_TOO_LARGE', message: 'The connection description exceeds the room message limit. Try a simpler network connection.' });
        fail(tooLarge); return Promise.reject(tooLarge);
      }
      queue = queue.then(function () { if (!done) return lobby.send(packet); });
      queue.catch(fail);
      return queue;
    }
    async function sendDescription(description) {
      await pc.setLocalDescription(description);
      if (done) return;
      if (pc.iceGatheringState !== 'complete') await new Promise(function (resolve, reject) {
        gathered = function (error) {
          globalThis.clearTimeout(gatherTimer); gathered = null;
          if (error) reject(error); else resolve();
        };
        gatherTimer = globalThis.setTimeout(function () {
          // Some mobile interfaces keep gathering after useful addresses exist.
          // Send that bounded snapshot once; do not add a trickle/retry loop.
          var hasCandidate = pc.localDescription && /^a=candidate:\S+/m.test(pc.localDescription.sdp);
          if (gathered) gathered(hasCandidate ? undefined : errorFrom({ code: 'PEER_GATHER_TIMEOUT', message: 'Network address discovery timed out. Try another network or return to solo play.' }));
        }, 8000);
      });
      if (!done) await sendSignal('description', { type: pc.localDescription.type, sdp: pc.localDescription.sdp });
    }
    function attach(channel) {
      if (done || dataChannel) { channel.close(); return; }
      dataChannel = channel;
      channel.onopen = function () {
        if (done || opened) return;
        opened = true; globalThis.clearTimeout(deadline); globalThis.clearTimeout(helloTimer);
        // Control/expiry remains checked by the parent panel; game data no longer polls the room.
        if (stop) { stop(); stop = null; }
        state('connected');
        resolvePeer({ role: role, send: function (data) {
          if (done || channel.readyState !== 'open') throw errorFrom({ code: 'PEER_CLOSED', message: 'The other player has disconnected.' });
          if (!validData(data)) throw errorFrom({ code: 'PEER_PAYLOAD_INVALID', message: 'Use finite JSON data no larger than 8192 UTF-8 bytes.' });
          if (channel.bufferedAmount > 262144) throw errorFrom({ code: 'PEER_BUSY', message: 'The connection is busy. Send fewer updates.' });
          channel.send(JSON.stringify(data));
        }, close: function () { finish(errorFrom({ code: 'PEER_CLOSED', message: 'You left the direct game.' })); } });
      };
      channel.onmessage = function (event) {
        if (done || typeof event.data !== 'string' || new TextEncoder().encode(event.data).byteLength > 8192) return;
        var data; try { data = JSON.parse(event.data); } catch (_) { return; }
        if (!validData(data) || typeof options.onMessage !== 'function') return;
        try { options.onMessage(data); } catch (_) { /* game callback cannot break transport cleanup */ }
      };
      channel.onclose = function () { finish(errorFrom({ code: 'PEER_CLOSED', message: 'The other player left the game.' })); };
      channel.onerror = function () { fail(); };
    }
    function createConnection() {
      pc = new globalThis.RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.cloudflare.com:3478' }], iceCandidatePoolSize: 0 });
      pc.onicecandidate = function (event) {
        if (done) return;
        if (!event.candidate) { if (gathered) gathered(); return; }
        if (++candidateCount > 64) return fail(errorFrom({ code: 'PEER_CANDIDATE_LIMIT', message: 'The connection produced too many network candidates.' }));
      };
      pc.onicegatheringstatechange = function () { if (pc.iceGatheringState === 'complete' && gathered) gathered(); };
      pc.ondatachannel = function (event) { attach(event.channel); };
      pc.onconnectionstatechange = function () {
        if (pc.connectionState === 'failed' || pc.connectionState === 'closed') return fail();
        globalThis.clearTimeout(disconnectedTimer);
        if (pc.connectionState === 'disconnected') disconnectedTimer = globalThis.setTimeout(function () { fail(); }, 10000);
      };
      state('connecting');
    }
    async function drainCandidates() {
      if (!pc || !pc.remoteDescription) return;
      while (candidates.length && !done) {
        var candidate = candidates.shift();
        if (candidate.hostSession === hostSession) await pc.addIceCandidate(candidate.value);
      }
    }
    async function receive(message) {
      if (done || opened || !message || !message.sender || message.sender.id === me) return;
      var packet = message.data;
      if (!packet || packet.protocol !== 'lob7.peer.v1' || typeof packet.session !== 'string' || packet.session.length !== 32
        || typeof packet.hostSession !== 'string' || packet.hostSession.length !== 32) return;
      if (role === 'guest' && packet.kind === 'ready' && message.sender.id === host && packet.to === null && !offered) {
        if (hostSession !== packet.hostSession) {
          hostSession = packet.hostSession;
          globalThis.clearTimeout(helloTimer);
          // Coalesce a page of old readiness messages: only answer the last one.
          helloTimer = globalThis.setTimeout(announce, 0);
        }
        return;
      }
      if (role === 'guest' && !offered && message.sender.id === host && packet.to === me
        && packet.session === session && acknowledgedHosts.has(packet.hostSession)) {
        // A late readiness packet from an older attempt must not displace a
        // valid offer replying to a hello sent during this guest attempt.
        if (packet.kind === 'description' && packet.value && packet.value.type === 'offer') hostSession = packet.hostSession;
        else if (packet.kind === 'candidate') {
          if (packet.value && typeof packet.value.candidate === 'string' && packet.value.candidate.length <= 4096 && candidates.length < 64)
            candidates.push({ hostSession: packet.hostSession, value: packet.value });
          return;
        }
      }
      if (packet.hostSession !== hostSession) return;
      if (role === 'host' && packet.kind === 'hello' && packet.to === host && !offered) {
        remote = message.sender.id; session = packet.session; offered = true;
        globalThis.clearTimeout(helloTimer);
        createConnection(); attach(pc.createDataChannel('lob7-data', { ordered: true }));
        await sendDescription(await pc.createOffer());
        return;
      }
      if (message.sender.id !== remote || packet.to !== me || packet.session !== session) return;
      if (packet.kind === 'candidate') {
        if (!packet.value || typeof packet.value.candidate !== 'string' || packet.value.candidate.length > 4096 || candidates.length >= 64) return;
        candidates.push({ hostSession: packet.hostSession, value: packet.value }); await drainCandidates();
      } else if (packet.kind === 'description') {
        var description = packet.value;
        if (!description || typeof description.sdp !== 'string' || description.sdp.length > 7900) return;
        if (role === 'guest' && description.type === 'offer' && !offered) {
          offered = true; globalThis.clearTimeout(helloTimer); createConnection();
          await pc.setRemoteDescription(description); await drainCandidates();
          await sendDescription(await pc.createAnswer());
        } else if (role === 'host' && description.type === 'answer' && !pc.remoteDescription) {
          await pc.setRemoteDescription(description); await drainCandidates();
        }
      }
    }
    function announce() {
      if (done || offered) return;
      if (role === 'host') void sendSignal('ready');
      else if (hostSession) {
        acknowledgedHosts.add(hostSession);
        if (acknowledgedHosts.size > 16) acknowledgedHosts.delete(acknowledgedHosts.values().next().value);
        void sendSignal('hello');
      }
      helloTimer = globalThis.setTimeout(announce, 6000);
    }
    try {
      if (signal && signal.aborted) { cancel(); return promise; }
      if (signal) signal.addEventListener('abort', cancel, { once: true });
      removeDisconnect = onDisconnect(finish);
      stop = lobby.subscribe(function (message) { return receive(message).catch(fail); }, { after: Number.isSafeInteger(room.lastSequence) ? room.lastSequence : 0, onError: fail });
      deadline = globalThis.setTimeout(function () {
        finish(errorFrom(pc
          ? { code: 'PEER_CONNECTION_TIMEOUT', message: failureText }
          : { code: 'PEER_WAITING_TIMEOUT', message: 'No partner has connected yet. Share the invitation and try again when they are ready.' }));
      }, timeout);
      state('waiting'); if (role === 'host') announce();
    } catch (error) { fail(error); }
    return promise;
  };
})();
