// Сүхбаатарын талбайн тулаан 3D — олон тоглогчийн сервер (Express + Socket.IO)
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const ROUND_MS = 180 * 1000;               // нэг раунд = 3 минут
const BREAK_KILL_MS = 5000;                 // Кадра алагдсаны дараах завсарлага
const BREAK_TIMEOUT_MS = 3000;              // хугацаа дууссаны дараах завсарлага

// одоогийн раунд: дугаар ба эхлэх цаг. Кадра алагдмагц раунд шууд дуусна.
let rs = { round: Math.floor(Date.now() / 1000), start: Date.now() + 2000 };
function nextRound(breakMs) {
  rs = { round: rs.round + 1, start: Date.now() + breakMs };
  io.emit('round', rs);
  setCup({ round: rs.round, phase: 'kadra' });
}
const CUP_HOLD_MS = 10 * 1000;              // цомыг 10 секунд хамгаалбал ялна
// цомын төлөв: kadra (Кадра амьд) → dropped (газарт) → carried (хэн нэгний гарт) → won
let cup = { round: rs.round, phase: 'kadra' };
function setCup(c) { cup = c; io.emit('cup', cup); }
function dropFromCarrier() {
  const p = players.get(cup.carrier);
  const x = p && p.state ? p.state.x : cup.x, z = p && p.state ? p.state.z : cup.z;
  setCup({ round: cup.round, phase: 'dropped', x, z, at: Date.now() });
}

const app = express();
const VERSION = 'v6';
const fs = require('fs');
// Тоглоомын файлыг public/index.html эсвэл repo-гийн үндсэн index.html-ээс хайж, аль шинэ хувилбарыг нь сонгоно
function pageInfo(file) {
  try {
    const html = fs.readFileSync(file, 'utf8');
    const m = html.match(/id="ver">v(\d+)</);
    return { file, html, ver: m ? +m[1] : 0 };
  } catch (e) { return null; }
}
const pages = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].map(pageInfo).filter(Boolean);
const page = pages.sort((a, b) => b.ver - a.ver)[0];
const sendPage = (req, res) => {
  if (!page) return res.status(404).send('index.html олдсонгүй — public/index.html файлаа шалгана уу');
  res.setHeader('Cache-Control', 'no-cache'); res.type('html').send(page.html);
};
app.get('/', sendPage);
app.get('/index.html', sendPage);
app.use(express.static(path.join(__dirname, 'public'), { setHeaders: res => res.setHeader('Cache-Control', 'no-cache') }));
app.get('/health', (req, res) => res.type('text').send(
  'ok ' + VERSION + '\n' +
  'тоглоом: ' + (page ? 'v' + page.ver + ' (' + path.relative(__dirname, page.file) + ')' : 'ОЛДСОНГҮЙ') + '\n' +
  'олдсон файлууд: ' + pages.map(p => path.relative(__dirname, p.file) + ' = v' + p.ver).join(', ')
));

const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 8 * 1024 });

const players = new Map();                  // socket.id -> { pid, name, state }
const rounds = [];                          // [{ round, winner, name, at }] — шинэ нь эхэндээ
const winnerOf = new Map();                 // round -> бичлэг

const str = (v, n = 16) => String(v ?? '').slice(0, n);
const num = v => (Number.isFinite(+v) ? +v : 0);

io.on('connection', socket => {
  players.set(socket.id, { pid: null, name: 'Тоглогч', state: null });
  socket.emit('welcome', { now: Date.now(), id: socket.id, rounds, rs, cup });

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

  // Кадра алагдаж цом унав (хамгийн түрүүнд ирсэн мэдээг авна)
  socket.on('cup:drop', d => {
    if (!d || num(d.round) !== rs.round || cup.round !== rs.round || cup.phase !== 'kadra' || Date.now() < rs.start) return;
    const lim = 200;
    setCup({ round: rs.round, phase: 'dropped', x: Math.max(-lim, Math.min(lim, num(d.x))), z: Math.max(-lim, Math.min(lim, num(d.z))), at: Date.now() });
  });
  // цом авах: тоглогч үнэхээр цомын дэргэд байгаа эсэхийг сервер шалгана
  socket.on('cup:pick', () => {
    if (cup.phase !== 'dropped' || cup.round !== rs.round) return;
    const p = players.get(socket.id);
    if (!p || !p.state || p.state.ko) return;
    if (Math.hypot(p.state.x - cup.x, p.state.z - cup.z) > 5) return;
    setCup({ round: cup.round, phase: 'carried', x: cup.x, z: cup.z, carrier: socket.id, cpid: p.pid || socket.id, cname: p.name, since: Date.now() });
  });
  // цом барьсан тоглогч алагдав
  socket.on('cup:lose', d => {
    if (cup.phase !== 'carried' || cup.carrier !== socket.id) return;
    setCup({ round: cup.round, phase: 'dropped', x: num(d && d.x), z: num(d && d.z), at: Date.now() });
  });

  socket.on('disconnect', () => {
    if (cup.phase === 'carried' && cup.carrier === socket.id) dropFromCarrier();
    players.delete(socket.id);
  });
});

// бүх тоглогчийн байрлалыг секундэд 10 удаа илгээнэ
setInterval(() => {
  const peers = [];
  for (const [id, p] of players) if (p.state) peers.push({ peer: id, presence: p.state });
  io.volatile.emit('peers', peers);
}, 100);

// цом барьсан тоглогч 10 секунд амьд үлдвэл ялна
setInterval(() => {
  if (cup.phase !== 'carried') return;
  const p = players.get(cup.carrier);
  if (!p || (p.state && p.state.ko)) { dropFromCarrier(); return; }
  if (Date.now() - cup.since >= CUP_HOLD_MS) {
    const rec = { round: cup.round, winner: cup.cpid, name: cup.cname, at: Date.now() };
    winnerOf.set(cup.round, rec);
    rounds.unshift(rec); if (rounds.length > 1000) rounds.pop();
    setCup({ ...cup, phase: 'won' });
    io.emit('roundWon', rec);
    io.emit('rounds', rounds);
    nextRound(BREAK_KILL_MS);
  }
}, 200);

// 3 минут дуусч хэн ч алаагүй бол дараагийн раунд
setInterval(() => { if (Date.now() > rs.start + ROUND_MS) nextRound(BREAK_TIMEOUT_MS); }, 500);

// хуучин раундын бичлэгийг цэвэрлэнэ
setInterval(() => { for (const k of winnerOf.keys()) if (k < rs.round - 5) winnerOf.delete(k); }, 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Сервер ажиллаж байна: порт ' + PORT));
