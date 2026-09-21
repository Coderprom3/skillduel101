// Skillcard Duel — matchmaking + web server

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  // Show the game when someone visits the website
  if (req.url === '/' || req.url === '/game.html') {
    const filePath = path.join(__dirname, 'game.html');

    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Could not load game.html');
        return;
      }

      res.writeHead(200, {
        'Content-Type': 'text/html; charset=UTF-8'
      });

      res.end(data);
    });

    return;
  }

  // Server status
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Skillcard Duel matchmaking server is running.');
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

const wss = new WebSocketServer({ server });

let queue = [];
const matches = new Map();

function safeSend(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try {
      ws.send(JSON.stringify(obj));
    } catch (e) {}
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

    if (a.readyState !== a.OPEN) {
      if (b.readyState === b.OPEN) queue.unshift(b);
      continue;
    }

    if (b.readyState !== b.OPEN) {
      queue.unshift(a);
      continue;
    }

    const matchId = Math.random().toString(36).slice(2, 10);

    matches.set(a, b);
    matches.set(b, a);

    safeSend(a, {
      type: 'matched',
      role: 'host',
      matchId
    });

    safeSend(b, {
      type: 'matched',
      role: 'guest',
      matchId
    });

    console.log(`Matched ${matchId}`);
  }
}

function disconnectFromMatch(ws) {
  const opponent = matches.get(ws);

  if (opponent) {
    safeSend(opponent, {
      type: 'opponent_left'
    });

    matches.delete(opponent);
  }

  matches.delete(ws);
}

wss.on('connection', (ws) => {
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data) => {
    let msg;

    try {
      msg = JSON.parse(data);
    } catch (e) {
      return;
    }

    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'find_match') {
      if (!queue.includes(ws) && !matches.has(ws)) {
        queue.push(ws);
        pairWaitingPlayers();
      }
    }

    else if (msg.type === 'cancel_find') {
      removeFromQueue(ws);
    }

    else if (msg.type === 'relay') {
      const opponent = matches.get(ws);

      if (opponent) {
        safeSend(opponent, {
          type: 'relay',
          payload: msg.payload
        });
      }
    }

    else if (msg.type === 'leave') {
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

// Keep WebSocket connections alive
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }

    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

server.listen(PORT, () => {
  console.log('Skillcard Duel server listening on port ' + PORT);
});
