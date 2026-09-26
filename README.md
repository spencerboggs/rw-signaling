# rw-signaling

WebSocket meeting place for the online raid wheel. Wheel data goes peer to peer after the connection is up. This server only creates rooms and passes WebRTC offers, answers, and ICE candidates.

Connect to `wss://<deployment>/api/ws`.

A normal GET on that path returns `{ "ok": true, "service": "rw-signaling" }`.

Rooms live in the function instance that created them. A join that misses the room should open a new socket and try again. Fluid compute needs to be on. Connections close when the function hits its max duration, then the client reconnects and rejoins.

Client messages:

- `create-room`
- `join-room` with `roomId`
- `signal` with `targetId` and `data` (`offer`, `answer`, or `ice`)

Server messages:

- `connected`
- `room-created`
- `room-joined` with `peers`
- `peer-joined`
- `peer-left`
- `signal` with `fromId` and `data`
- `error`
