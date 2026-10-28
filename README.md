# Prism Pair

An original, short cooperative game prepared as the first two-player LOB7 Rooms
example. Move a cyan light and an amber light into their matching rings; hold
both in place together to add a star to the constellation. The host simulates
the game and sends snapshots; the guest sends only its input.

## Status

Source example, not a published LOB7 game. No public repository or hosted match
is claimed here. Deployment and publication require Cédric's approval.

## Try solo

Serve this folder with a local static server, open `index.html`, and choose
**Solo rehearsal**. The amber light becomes a practice partner. Arrow keys,
A/D, and pointer/touch dragging move your light. There are no external assets,
fonts, dependencies, build step, microphone, or camera.

## Two players after authorized publication

Publish these files as their own public GitHub repository, then prepare the
exact commit on LOB7 using `LOB7-GAME.json`. In the LOB7 player, create or join
a two-player room and explicitly accept the IP-address notice. Each player
then clicks **Connect selected room** inside the game. The host keeps the game
tab open. Leaving, closing the room, or a failed connection ends the match.

`lob7-rooms.js` and `lob7-rooms-p2p.js` are unmodified copies of the platform SDK
and optional direct-play helper from `web/public/`. Refresh both when the SDK changes.
Load the base SDK first, then the P2P helper, then the game. Games declaring
`multiplayer.mode: "room"` should load only the base SDK. The game uses
`LOB7Rooms.connect()` followed by `room.connectPeer(...)`. No account token,
email, or account pseudonym enters the game. Room-local participant identifiers
are used by the SDK. Cloudflare STUN at `stun.cloudflare.com:3478` assists
connection setup; there is no TURN relay. Direct connections can fail on some
networks. Signals are bounded room messages; snapshots use the data channel.

The demo uses one host, one guest, one ordered data channel and a 90-second
connection deadline. It has no host migration, automatic reconnection, durable
game state, or third-party assets. EN/FR/ES are selected explicitly in the game.

MIT-licensed original example by Nilo, with AI assistance, for LOB7.
