import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const app = express();
const server = createServer(app);

const wss = new WebSocketServer({ server });

interface Client {
    socket: WebSocket;
    roomId?: string;
    clientId: string;
}

const clients = new Map<string, Client>();

const rooms = new Map<string, Set<string>>();

function send(socket: WebSocket, message: unknown) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(message));
    }
}

function broadcast(roomId: string, message: unknown, exceptId?: string) {
    const room = rooms.get(roomId);

    if (!room) {
        return;
    }

    for (const clientId of room) {
        if (clientId === exceptId) {
            continue;
        }

        const client = clients.get(clientId);

        if (client) {
            send(client.socket, message);
        }
    }
}

function removeClient(clientId: string) {
    const client = clients.get(clientId);

    if (!client) {
        return;
    }

    if (client.roomId) {
        const room = rooms.get(client.roomId);

        if (room) {
            room.delete(clientId);

            broadcast(
                client.roomId,
                {
                    type: 'peer-left',
                    clientId,
                },
                clientId,
            );

            if (room.size === 0) {
                rooms.delete(client.roomId);
            }
        }
    }

    clients.delete(clientId);
}

wss.on('connection', (socket) => {
    const clientId = crypto.randomUUID();

    const client: Client = {
        socket,
        clientId,
    };

    clients.set(clientId, client);

    send(socket, {
        type: 'connected',
        clientId,
    });

    socket.on('message', (raw) => {
        let message: any;

        try {
            message = JSON.parse(raw.toString());
        } catch {
            send(socket, {
                type: 'error',
                message: 'Invalid JSON.',
            });

            return;
        }

        switch (message.type) {
            case 'create-room': {
                const roomId = crypto.randomUUID();

                client.roomId = roomId;

                rooms.set(roomId, new Set([clientId]));

                send(socket, {
                    type: 'room-created',
                    roomId,
                    clientId,
                });

                break;
            }

            case 'join-room': {
                const roomId = message.roomId;

                if (typeof roomId !== 'string') {
                    send(socket, {
                        type: 'error',
                        message: 'Invalid room ID.',
                    });

                    return;
                }

                const room = rooms.get(roomId);

                if (!room) {
                    send(socket, {
                        type: 'error',
                        message: 'Room does not exist.',
                    });

                    return;
                }

                client.roomId = roomId;

                const existingPeers = [...room];

                room.add(clientId);

                send(socket, {
                    type: 'room-joined',
                    roomId,
                    clientId,
                    peers: existingPeers,
                });

                broadcast(
                    roomId,
                    {
                        type: 'peer-joined',
                        clientId,
                    },
                    clientId,
                );

                break;
            }

            case 'signal': {
                const targetId = message.targetId;

                if (typeof targetId !== 'string') {
                    return;
                }

                const target = clients.get(targetId);

                if (!target) {
                    send(socket, {
                        type: 'error',
                        message: 'Target client is no longer connected.',
                    });

                    return;
                }

                send(target.socket, {
                    type: 'signal',
                    fromId: clientId,
                    data: message.data,
                });

                break;
            }

            default:
                send(socket, {
                    type: 'error',
                    message: `Unknown message type: ${message.type}`,
                });
        }
    });

    socket.on('close', () => {
        removeClient(clientId);
    });

    socket.on('error', () => {
        removeClient(clientId);
    });
});

export default server;