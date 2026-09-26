import { randomInt, randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { WebSocketServer, WebSocket } from 'ws'

const ROOM_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
const MAX_PEERS = 8
const MAX_SDP = 20000

interface Client {
  socket: WebSocket
  roomId?: string
  clientId: string
  alive: boolean
}

const clients = new Map<string, Client>()
const rooms = new Map<string, Set<string>>()

const server = createServer((_req, res) => {
  res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' })
  res.end(JSON.stringify({ ok: true, service: 'rw-signaling' }))
})

const wss = new WebSocketServer({ server, maxPayload: 32 * 1024 })

function send(socket: WebSocket, message: unknown) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message))
}

function broadcast(roomId: string, message: unknown, exceptId?: string) {
  const room = rooms.get(roomId)
  if (!room) return
  for (const clientId of room) {
    if (clientId === exceptId) continue
    const client = clients.get(clientId)
    if (client) send(client.socket, message)
  }
}

function leaveRoom(client: Client) {
  if (!client.roomId) return
  const room = rooms.get(client.roomId)
  const roomId = client.roomId
  client.roomId = undefined
  if (!room) return
  room.delete(client.clientId)
  broadcast(roomId, { type: 'peer-left', clientId: client.clientId }, client.clientId)
  if (room.size === 0) rooms.delete(roomId)
}

function removeClient(clientId: string) {
  const client = clients.get(clientId)
  if (!client) return
  leaveRoom(client)
  clients.delete(clientId)
}

function roomCode(): string {
  let code = ''
  do {
    code = ''
    for (let i = 0; i < 4; i += 1) code += ROOM_ALPHABET[randomInt(ROOM_ALPHABET.length)]
  } while (rooms.has(code))
  return code
}

function signalData(data: unknown) {
  if (!data || typeof data !== 'object') return null
  const record = data as { kind?: unknown; sdp?: unknown; candidate?: unknown }
  if ((record.kind === 'offer' || record.kind === 'answer') && typeof record.sdp === 'string' && record.sdp.length <= MAX_SDP) {
    return { kind: record.kind, sdp: record.sdp }
  }
  if (record.kind === 'ice' && record.candidate && typeof record.candidate === 'object') {
    return { kind: 'ice', candidate: record.candidate }
  }
  return null
}

wss.on('connection', (socket) => {
  const clientId = randomUUID()
  const client: Client = { socket, clientId, alive: true }
  clients.set(clientId, client)
  send(socket, { type: 'connected', clientId })

  socket.on('pong', () => {
    client.alive = true
  })

  socket.on('message', (raw) => {
    let message: { type?: string; roomId?: unknown; targetId?: unknown; data?: unknown }
    try {
      message = JSON.parse(raw.toString())
    } catch {
      send(socket, { type: 'error', message: 'Invalid JSON.' })
      return
    }

    switch (message.type) {
      case 'create-room': {
        leaveRoom(client)
        const roomId = roomCode()
        client.roomId = roomId
        rooms.set(roomId, new Set([clientId]))
        send(socket, { type: 'room-created', roomId, clientId })
        break
      }
      case 'join-room': {
        const roomId = typeof message.roomId === 'string' ? message.roomId.trim().toUpperCase() : ''
        const room = rooms.get(roomId)
        if (!room) {
          send(socket, { type: 'error', message: 'Room does not exist.' })
          return
        }
        if (room.has(clientId)) {
          send(socket, { type: 'room-joined', roomId, clientId, peers: [...room].filter((id) => id !== clientId) })
          return
        }
        if (room.size >= MAX_PEERS) {
          send(socket, { type: 'error', message: 'Room is full.' })
          return
        }
        leaveRoom(client)
        const existingPeers = [...room]
        room.add(clientId)
        client.roomId = roomId
        send(socket, { type: 'room-joined', roomId, clientId, peers: existingPeers })
        broadcast(roomId, { type: 'peer-joined', clientId }, clientId)
        break
      }
      case 'signal': {
        const targetId = message.targetId
        if (typeof targetId !== 'string') return
        const data = signalData(message.data)
        const target = clients.get(targetId)
        if (!data || !target || !client.roomId || target.roomId !== client.roomId) {
          send(socket, { type: 'error', message: 'Target client is no longer connected.' })
          return
        }
        send(target.socket, { type: 'signal', fromId: clientId, data })
        break
      }
      default:
        send(socket, { type: 'error', message: 'Unknown message type.' })
    }
  })

  socket.on('close', () => {
    removeClient(clientId)
  })

  socket.on('error', () => {
    removeClient(clientId)
  })
})

const beat = setInterval(() => {
  for (const client of clients.values()) {
    if (!client.alive) {
      client.socket.terminate()
      continue
    }
    client.alive = false
    try {
      client.socket.ping()
    } catch {
      client.socket.terminate()
    }
  }
}, 25000)

server.on('close', () => clearInterval(beat))

export default server
