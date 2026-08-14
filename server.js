const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;

// Start the WebSocket server
const wss = new WebSocket.Server({ port: PORT });
console.log(`Signaling server running on port ${PORT}`);

// Dictionary to hold our active transfers
// Format: sessions[code] = { sender: WebSocket, receiver: WebSocket }
const sessions = {};

wss.on('connection', (ws) => {
    let currentCode = null;
    let role = null; // 'sender' or 'receiver'

    ws.on('message', (messageAsString) => {
        const message = JSON.parse(messageAsString);

        switch (message.type) {
            case 'create':
                // Sender generates a code and opens a room
                currentCode = message.code;
                role = 'sender';
                sessions[currentCode] = { sender: ws, receiver: null };
                console.log(`Room ${currentCode} created by sender.`);
                break;

            case 'join':
                // Receiver types the code and joins the room
                currentCode = message.code;
                role = 'receiver';
                
                if (sessions[currentCode]) {
                    sessions[currentCode].receiver = ws;
                    console.log(`Receiver joined room ${currentCode}.`);
                    
                    // Tell the sender that the receiver has arrived so they can start the WebRTC handshake
                    sessions[currentCode].sender.send(JSON.stringify({ type: 'peer-joined' }));
                } else {
                    ws.send(JSON.stringify({ type: 'error', message: 'Transfer code not found.' }));
                }
                break;

            case 'signal':
                // Pass WebRTC connection data (SDP offers/answers and ICE candidates) to the other device
                if (sessions[currentCode]) {
                    const target = role === 'sender' ? sessions[currentCode].receiver : sessions[currentCode].sender;
                    
                    if (target && target.readyState === WebSocket.OPEN) {
                        target.send(JSON.stringify({
                            type: 'signal',
                            data: message.data
                        }));
                    }
                }
                break;
        }
    });

    // Handle disconnections safely
    ws.on('close', () => {
        if (currentCode && sessions[currentCode]) {
            console.log(`${role.toUpperCase()} disconnected from room ${currentCode}.`);
            
            // Notify the other peer so they can show an error or reset their UI
            const target = role === 'sender' ? sessions[currentCode].receiver : sessions[currentCode].sender;
            if (target && target.readyState === WebSocket.OPEN) {
                target.send(JSON.stringify({ type: 'peer-disconnected' }));
            }
            
            // Clean up the room to prevent memory leaks
            delete sessions[currentCode];
        }
    });
});