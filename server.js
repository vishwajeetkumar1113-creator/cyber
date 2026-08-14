const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;

// Start the WebSocket server
const wss = new WebSocket.Server({ port: PORT });
console.log(`Signaling server running on port ${PORT}`);

// Dictionary to hold our active transfers
const sessions = {};

wss.on('connection', (ws) => {
    let currentCode = null;
    let role = null; // 'sender' or 'receiver'

    ws.on('message', (messageAsString) => {
        let message;
        try {
            message = JSON.parse(messageAsString);
        } catch (e) {
            return; // Ignore invalid JSON
        }

        switch (message.type) {
            case 'create':
                currentCode = message.code;
                role = 'sender';
                sessions[currentCode] = { sender: ws, receiver: null };
                console.log(`Room ${currentCode} created by sender.`);
                break;

            case 'join':
                currentCode = message.code;
                role = 'receiver';
                
                if (sessions[currentCode]) {
                    sessions[currentCode].receiver = ws;
                    console.log(`Receiver joined room ${currentCode}.`);
                    
                    // Tell the sender that the receiver has arrived
                    sessions[currentCode].sender.send(JSON.stringify({ type: 'peer-joined' }));
                } else {
                    ws.send(JSON.stringify({ type: 'error', message: 'Transfer code not found or expired.' }));
                }
                break;

            case 'signal':
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

            // 🔥 NAYA FALLBACK RELAY LOGIC: WebRTC fail hone par WebSocket ke through data pass karne ke liye
            case 'relay-chunk':
                if (sessions[currentCode]) {
                    const target = role === 'sender' ? sessions[currentCode].receiver : sessions[currentCode].sender;
                    if (target && target.readyState === WebSocket.OPEN) {
                        target.send(messageAsString);
                    }
                }
                break;
        }
    });

    // Handle disconnections safely
    ws.on('close', () => {
        if (currentCode && sessions[currentCode]) {
            console.log(`${role.toUpperCase()} disconnected from room ${currentCode}.`);
            
            if (role === 'sender') {
                // Agar SENDER gaya, toh room delete kar do
                const receiver = sessions[currentCode].receiver;
                if (receiver && receiver.readyState === WebSocket.OPEN) {
                    receiver.send(JSON.stringify({ type: 'peer-disconnected' }));
                }
                delete sessions[currentCode];
                console.log(`Room ${currentCode} deleted because sender left.`);
            } 
            else if (role === 'receiver') {
                // Agar RECEIVER gaya (jaise page refresh kiya), toh room bacha ke rakho!
                const sender = sessions[currentCode].sender;
                if (sender && sender.readyState === WebSocket.OPEN) {
                    sender.send(JSON.stringify({ type: 'peer-disconnected' }));
                }
                // Room delete mat karo, bas receiver ki jagah khali kar do
                sessions[currentCode].receiver = null;
                console.log(`Room ${currentCode} kept alive. Waiting for receiver to return.`);
            }
        }
    });
});
