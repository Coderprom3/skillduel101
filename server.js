// Skillcard Duel — matchmaking + relay server
//
// What this does:
//  - Players connect over WebSocket and send { type: 'find_match' }
//  - The server pairs up any two waiting players and tells them who's
//    "host" and who's "guest"
//  - After that, it just relays whatever messages the two players send
//    each other — it never looks at or understands game rules, HP,
//    cards, etc. All of that logic still lives entirely in the browser,
//    exactly like the old peer-to-peer version. This server only solves
//    the "how do two strangers find each other" problem.
//
// Run locally:   npm install && npm start
// Deploy:        see README.md

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Skillcard Duel matchmaking server is running.\n');
});

const wss = new WebSocketServer({ server });

let queue = [];               // sockets waiting for an opponent
const matches = new Map();    // ws -> opponent ws

function safeSend(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) { /* ignore */ }
  }
}

function removeFromQueue(ws) {
  const idx = queue.indexOf(ws);
  if (idx !== -1) queue.splice(idx, 1);
}

function pairWaitingPlayers() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();
    if (a.readyState !== a.OPEN) { if (b.readyState === b.OPEN) queue.unshift(b); continue; }
    if (b.readyState !== b.OPEN) { queue.unshift(a); continue; }

    const matchId = Math.random().toString(36).slice(2, 10);
    matches.set(a, b);
    matches.set(b, a);
    safeSend(a, { type: 'matched', role: 'host', matchId });
    safeSend(b, { type: 'matched', role: 'guest', matchId });
    console.log(`Matched ${matchId}`);
  }
}

function disconnectFromMatch(ws) {
  const opponent = matches.get(ws);
  if (opponent) {
    safeSend(opponent, { type: 'opponent_left' });
    matches.delete(opponent);
  }
  matches.delete(ws);
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'find_match') {
      if (!queue.includes(ws) && !matches.has(ws)) {
        queue.push(ws);
        pairWaitingPlayers();
      }
    } else if (msg.type === 'cancel_find') {
      removeFromQueue(ws);
    } else if (msg.type === 'relay') {
      const opponent = matches.get(ws);
      if (opponent) safeSend(opponent, { type: 'relay', payload: msg.payload });
    } else if (msg.type === 'leave') {
      disconnectFromMatch(ws);
    }
  });

  ws.on('close', () => {
    removeFromQueue(ws);
    disconnectFromMatch(ws);
  });

  ws.on('error', () => {
    removeFromQueue(ws);
    disconnectFromMatch(ws);
  });
});

// Ping clients periodically; drop any that stop responding (e.g. they
// closed the tab without a clean disconnect). Also keeps free-tier hosts
// from treating the connection as idle.
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('close', () => clearInterval(pingInterval));

server.listen(PORT, () => {
  console.log('Matchmaking server listening on port ' + PORT);
});
