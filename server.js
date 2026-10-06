// Сүхбаатарын талбайн тулаан 3D — олон тоглогчийн сервер (Express + Socket.IO)
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const ROUND_MS = 180 * 1000;               // нэг раунд = 3 минут
const roundIdx = () => Math.floor(Date.now() / ROUND_MS);

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (req, res) => res.send('ok'));

const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 8 * 1024 });

const players = new Map();                  // socket.id -> { pid, name, state }
const rounds = [];                          // [{ round, winner, name, at }] — шинэ нь эхэндээ
const winnerOf = new Map();                 // round -> бичлэг

const str = (v, n = 16) => String(v ?? '').slice(0, n);
const num = v => (Number.isFinite(+v) ? +v : 0);

io.on('connection', socket => {
  players.set(socket.id, { pid: null, name: 'Тоглогч', state: null });
  socket.emit('welcome', { now: Date.now(), id: socket.id, rounds });

  socket.on('join', d => {
    const p = players.get(socket.id); if (!p || !d) return;
    p.pid = str(d.pid, 40) || socket.id;
    if (d.name) p.name = str(d.name);
  });

  // тоглогчийн байрлал, буу, буудлага (секундэд ~10 удаа)
  socket.on('state', s => {
    const p = players.get(socket.id); if (!p || !s || typeof s !== 'object') return;
    p.state = {
      n: str(s.n) || p.name, x: num(s.x), y: num(s.y), z: num(s.z), r: num(s.r), p: num(s.p),
      w: str(s.w, 10), ko: s.ko ? 1 : 0, sn: num(s.sn),
      ...(s.sx !== undefined ? { sx: num(s.sx), sy: num(s.sy), sz: num(s.sz) } : {})
    };
    if (p.state.n) p.name = p.state.n;
  });

  // онох (hit) ба алах (kill) мэдээг бусдад дамжуулна
  socket.on('ev', m => {
    if (!m || typeof m !== 'object') return;
    const topic = str(m.topic, 8);
    if (topic !== 'hit' && topic !== 'kill') return;
    const d = m.data && typeof m.data === 'object' ? m.data : {};
    const data = topic === 'hit'
      ? { to: str(d.to, 40), dmg: Math.max(0, Math.min(250, num(d.dmg))), n: str(d.n), w: str(d.w, 10) }
      : { by: str(d.by, 40), bn: str(d.bn), vn: str(d.vn), w: str(d.w, 10) };
    socket.broadcast.emit('ev:' + topic, { from: socket.id, data });
  });

  // Кадраг хэн түрүүлж алсныг сервер шийднэ
  socket.on('claim', d => {
    const R = roundIdx();
    if (!d || num(d.round) !== R || winnerOf.has(R)) return;
    const p = players.get(socket.id);
    const rec = { round: R, winner: str(d.winner, 40) || (p && p.pid) || socket.id, name: str(d.name) || (p ? p.name : 'Тоглогч'), at: Date.now() };
    winnerOf.set(R, rec);
    rounds.unshift(rec); if (rounds.length > 1000) rounds.pop();
    io.emit('roundWon', rec);
    io.emit('rounds', rounds);
  });

  socket.on('disconnect', () => players.delete(socket.id));
});

// бүх тоглогчийн байрлалыг секундэд 10 удаа илгээнэ
setInterval(() => {
  const peers = [];
  for (const [id, p] of players) if (p.state) peers.push({ peer: id, presence: p.state });
  io.volatile.emit('peers', peers);
}, 100);

// хуучин раундын бичлэгийг цэвэрлэнэ
setInterval(() => { const R = roundIdx(); for (const k of winnerOf.keys()) if (k < R - 5) winnerOf.delete(k); }, 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Сервер ажиллаж байна: порт ' + PORT));
