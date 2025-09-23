// index.js - Full PF Giveaway bot + Express dashboard API
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');

let fetchFunc = global.fetch;
if (!fetchFunc) {
  try { fetchFunc = require('node-fetch'); } catch (e) { fetchFunc = null; }
}

// discord.js
const { Client, GatewayIntentBits, Partials, EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle, PermissionFlagsBits, ChannelType } = require('discord.js');

const DATA_PATH = path.join(__dirname, 'pf_data.json');
const PORT = Number(process.env.PORT || 3000);
const TRON_GRID_API = (process.env.TRON_GRID_API || 'https://api.trongrid.io').replace(/\/$/, '');
const ADMIN_SECRET = process.env.ADMIN_SECRET || null;

// Colors
const COLOR_COUNTDOWN = 0x111827;
const COLOR_FETCHING = 0xf59e0b;
const COLOR_ENDED = 0x16a34a;

// --- persistence ---
const DEFAULT_DATA = { guilds: {}, requiredJoinRoleByGuild: {}, giveaways: [] };

function readData() {
  try {
    if (!fs.existsSync(DATA_PATH)) {
      fs.writeFileSync(DATA_PATH, JSON.stringify(DEFAULT_DATA, null, 2));
      return JSON.parse(JSON.stringify(DEFAULT_DATA));
    }
    const raw = fs.readFileSync(DATA_PATH, 'utf8') || '{}';
    const obj = JSON.parse(raw);
    obj.guilds = obj.guilds || {};
    obj.requiredJoinRoleByGuild = obj.requiredJoinRoleByGuild || {};
    obj.giveaways = Array.isArray(obj.giveaways) ? obj.giveaways : [];
    return obj;
  } catch (err) {
    console.error('readData error - recreating', err);
    fs.writeFileSync(DATA_PATH, JSON.stringify(DEFAULT_DATA, null, 2));
    return JSON.parse(JSON.stringify(DEFAULT_DATA));
  }
}
function writeData(data) {
  fs.writeFileSync(DATA_PATH + '.tmp', JSON.stringify(data, null, 2));
  fs.renameSync(DATA_PATH + '.tmp', DATA_PATH);
}

// --- utils ---
function random64() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < 64; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}
function hmacSha512Hex(key, msg) {
  return crypto.createHmac('sha512', key).update(msg).digest('hex');
}
function hmacHexToFloat(hex) {
  const first = hex.slice(0, 13);
  const num = parseInt(first, 16);
  return num / Math.pow(16, first.length);
}
function delay(ms) { return new Promise(res => setTimeout(res, ms)); }

// --- TRON helpers ---
async function getNowBlockRaw() {
  if (!fetchFunc) return null;
  try {
    const r = await fetchFunc(TRON_GRID_API + '/wallet/getnowblock');
    if (!r.ok) throw new Error('status ' + r.status);
    return await r.json();
  } catch (e) {
    console.warn('getNowBlockRaw failed', e && (e.message || e));
    return null;
  }
}
function extractBlockNumber(blockObj) {
  try {
    if (!blockObj) return null;
    if (blockObj.block_header && blockObj.block_header.raw_data && typeof blockObj.block_header.raw_data.number === 'number') return blockObj.block_header.raw_data.number;
    if (typeof blockObj.block_num === 'number') return blockObj.block_num;
    if (typeof blockObj.number === 'number') return blockObj.number;
    return null;
  } catch (e) { return null; }
}
async function getBlockByNumber(num) {
  if (!fetchFunc) return null;
  try {
    const r = await fetchFunc(TRON_GRID_API + '/wallet/getblockbynum', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ num }) });
    if (!r.ok) throw new Error('status ' + r.status);
    return await r.json();
  } catch (e) {
    console.warn('getBlockByNumber failed', e && (e.message || e));
    return null;
  }
}

// --- models & persistence wrappers ---
function newGiveawayObj({ guildId, channelId, hostId, isAllInOne, items }) {
  return {
    id: 'gw_' + Date.now() + '_' + Math.floor(Math.random() * 9000 + 1000),
    guildId,
    channelId,
    messageId: null,
    hostId: hostId || null,
    createdAt: Date.now(),
    isAllInOne: !!isAllInOne,
    serverPublicKey: random64(),
    items: items.map(it => ({
      id: it.id,
      prize: it.prize,
      endsAt: Number(it.endsAt || 0),
      winnersCount: Number(it.winnersCount || 1),
      requiredRole: it.requiredRole || null,
      entries: Array.isArray(it.entries) ? it.entries : [],
      hmacs: {},
      winners: Array.isArray(it.winners) ? it.winners : [],
      ended: !!it.ended,
      clientBlockID: it.clientBlockID || null,
      clientBlockNum: it.clientBlockNum || null,
      clientSeed: it.clientSeed || null,
      clientBlockNumTarget: it.clientBlockNumTarget || null,
      awaitingClientSeed: !!it.awaitingClientSeed
    }))
  };
}

function saveGiveaway(gw) {
  const d = readData();
  d.giveaways = d.giveaways || [];
  const idx = d.giveaways.findIndex(g => g.id === gw.id);
  if (idx === -1) d.giveaways.push(gw); else d.giveaways[idx] = gw;
  writeData(d);
}
function removeGiveawayById(gwId) {
  const d = readData();
  const idx = d.giveaways.findIndex(g => g.id === gwId);
  if (idx !== -1) {
    d.giveaways.splice(idx, 1);
    writeData(d);
  }
}
function findGiveawayById(gwId) {
  const d = readData();
  return (d.giveaways || []).find(g => g.id === gwId);
}
function listGiveaways() {
  const d = readData();
  return d.giveaways || [];
}

// --- awaiting worker queue ---
let processingAwaitingQueue = false;

async function enqueueAwaitingItem(gwId, itemId) {
  const gw = findGiveawayById(gwId);
  if (!gw) return;
  const item = gw.items.find(it => it.id === itemId);
  if (!item || item.ended) return;
  item.awaitingClientSeed = true;
  saveGiveaway(gw);
  processAwaitingQueue().catch(err => console.error('processAwaitingQueue err', err));
}

async function getNextAwaitingItem() {
  const d = readData();
  let best = null;
  for (const gw of d.giveaways || []) {
    for (const it of gw.items || []) {
      if (it.awaitingClientSeed && !it.ended) {
        const score = (it.clientBlockNumTarget || it.endsAt || Number.MAX_SAFE_INTEGER);
        if (!best || score < best.score) best = { gwId: gw.id, itemId: it.id, score };
      }
    }
  }
  return best;
}

async function processAwaitingQueue() {
  if (processingAwaitingQueue) return;
  processingAwaitingQueue = true;
  try {
    while (true) {
      const next = await getNextAwaitingItem();
      if (!next) break;
      const gw = findGiveawayById(next.gwId);
      if (!gw) continue;
      const item = gw.items.find(it => it.id === next.itemId);
      if (!item || item.ended || !item.awaitingClientSeed) continue;

      const targetNum = item.clientBlockNumTarget;
      console.log(`Worker: waiting for block ${targetNum || '(compute)'} for giveaway ${gw.id} item ${item.id}`);

      let success = false;
      while (!success) {
        try {
          let blockObj = null;
          let blockNum = null;
          if (typeof targetNum === 'number') {
            blockObj = await getBlockByNumber(targetNum);
            blockNum = targetNum;
          } else {
            const nowRaw = await getNowBlockRaw();
            const nowNum = extractBlockNumber(nowRaw);
            if (nowNum !== null) {
              blockNum = nowNum + 2;
              blockObj = await getBlockByNumber(blockNum);
            } else blockObj = null;
          }

          if (blockObj && (blockObj.blockID || (blockObj.block_header && blockObj.block_header.raw_data))) {
            const blockID = blockObj.blockID || (blockObj.block_header && blockObj.block_header.raw_data && blockObj.block_header.raw_data.value) || null;
            await finalizeItemWithFetchedBlock(gw, item, { blockID, blockNum, raw: blockObj });
            success = true; break;
          } else {
            await delay(10000);
            const gwNow = findGiveawayById(gw.id);
            if (!gwNow) { success = true; break; }
            const itNow = gwNow.items.find(i => i.id === item.id);
            if (!itNow || itNow.ended || !itNow.awaitingClientSeed) { success = true; break; }
          }
        } catch (err) {
          console.warn('Worker loop error', err && (err.message || err));
          await delay(10000);
        }
      }
    }
  } finally {
    processingAwaitingQueue = false;
  }
}

// --- finalize when block fetched ---
function pickTopNFromHmacs(hmap, n) {
  const arr = Object.entries(hmap || {}).map(([uid, info]) => ({ uid, float: info.float }));
  arr.sort((a, b) => b.float - a.float);
  return arr.slice(0, n).map(x => x.uid);
}
function pickRandom(entries, n) {
  const uniq = Array.from(new Set(entries || []));
  const winners = [];
  while (winners.length < n && uniq.length > 0) winners.push(uniq.splice(Math.floor(Math.random() * uniq.length), 1)[0]);
  return winners;
}

async function finalizeItemWithFetchedBlock(gw, item, target) {
  try {
    const gwFresh = findGiveawayById(gw.id) || gw;
    const itemFresh = gwFresh.items.find(it => it.id === item.id) || item;
    itemFresh.clientBlockID = target.blockID || itemFresh.clientBlockID || null;
    itemFresh.clientBlockNum = typeof target.blockNum === 'number' ? target.blockNum : itemFresh.clientBlockNum || null;
    itemFresh.clientSeed = itemFresh.clientBlockID || '(unavailable)';

    itemFresh.hmacs = itemFresh.hmacs || {};
    for (const uid of itemFresh.entries || []) {
      try {
        const mac = hmacSha512Hex(gwFresh.serverPublicKey || '', `${itemFresh.clientSeed}:${uid}`);
        const floatVal = hmacHexToFloat(mac);
        itemFresh.hmacs[uid] = { hmac: mac, float: floatVal };
      } catch (e) { console.warn('hmac compute fail for uid', e && (e.message || e)); }
    }

    const hasHmacs = Object.keys(itemFresh.hmacs || {}).length > 0;
    itemFresh.winners = hasHmacs ? pickTopNFromHmacs(itemFresh.hmacs, itemFresh.winnersCount) : pickRandom(itemFresh.entries || [], itemFresh.winnersCount);
    itemFresh.ended = true;
    itemFresh.awaitingClientSeed = false;

    saveGiveaway(gwFresh);

    // update embed
    try {
      const ch = await client.channels.fetch(gwFresh.channelId).catch(() => null);
      if (ch) {
        const msg = await ch.messages.fetch(gwFresh.messageId).catch(() => null);
        if (msg) {
          const embed = buildGiveawayEmbed(gwFresh);
          const rows = buildButtonRowsForGiveaway(gwFresh);
          await msg.edit({ embeds: [embed], components: rows }).catch(() => { });
        }
      }
    } catch (e) { /* ignore */ }

    // announce winners
    try {
      const ch = await client.channels.fetch(gwFresh.channelId).catch(() => null);
      if (ch) {
        const announce = itemFresh.winners && itemFresh.winners.length ? `🎉 **${itemFresh.prize}** — Winners: ${itemFresh.winners.map(id => `<@${id}>`).join(', ')}` : `🎁 **${itemFresh.prize}** — No valid entries`;
        await ch.send({ content: announce }).catch(() => { });
      }
    } catch (e) { /* ignore */ }

    console.log(`Finalized item ${itemFresh.id} (giveaway ${gwFresh.id}) winners:`, itemFresh.winners);
  } catch (err) {
    console.error('finalize error', err && (err.stack || err.message || err));
  }
}

// --- schedule / end ---
const itemTimers = new Map();
function scheduleItemEnd(gwId, itemId) {
  const gw = findGiveawayById(gwId);
  if (!gw) return;
  const it = gw.items.find(i => i.id === itemId);
  if (!it || it.ended) return;
  const now = Date.now();
  const ms = Math.max(0, (it.endsAt || now) - now);
  if (itemTimers.has(itemId)) { clearTimeout(itemTimers.get(itemId)); itemTimers.delete(itemId); }
  const t = setTimeout(() => {
    endItem(gwId, itemId).catch(err => console.error('endItem error', err));
  }, ms + 50);
  itemTimers.set(itemId, t);
}

async function endItem(gwId, itemId) {
  const gw = findGiveawayById(gwId);
  if (!gw) return;
  const item = gw.items.find(it => it.id === itemId);
  if (!item || item.ended) return;

  item.awaitingClientSeed = true;
  const nowraw = await getNowBlockRaw();
  const nowNum = extractBlockNumber(nowraw);
  if (typeof nowNum === 'number') item.clientBlockNumTarget = nowNum + 2;
  else item.clientBlockNumTarget = null;
  saveGiveaway(gw);
  await enqueueAwaitingItem(gwId, itemId);
}

// --- reroll handler ---
async function rerollItemHandler(gwId, itemId, resetFlag) {
  const gw = findGiveawayById(gwId);
  if (!gw) throw new Error('giveaway_not_found');
  const item = gw.items.find(it => it.id === itemId);
  if (!item) throw new Error('item_not_found');

  if (resetFlag) {
    item.clientSeed = null; item.clientBlockID = null; item.clientBlockNum = null; item.clientBlockNumTarget = null;
    item.hmacs = {}; item.winners = []; item.ended = false; item.awaitingClientSeed = true;
    const nowRaw = await getNowBlockRaw(); const nowNum = extractBlockNumber(nowRaw);
    if (typeof nowNum === 'number') item.clientBlockNumTarget = nowNum + 2; else item.clientBlockNumTarget = null;
    saveGiveaway(gw);
    await enqueueAwaitingItem(gwId, itemId);
    return { status: 'reset_enqueued' };
  }

  if (item.ended && item.clientSeed) {
    item.hmacs = {};
    for (const uid of item.entries || []) {
      try { const mac = hmacSha512Hex(gw.serverPublicKey || '', `${item.clientSeed}:${uid}`); const floatVal = hmacHexToFloat(mac); item.hmacs[uid] = { hmac: mac, float: floatVal }; } catch (e) {}
    }
    item.winners = Object.keys(item.hmacs).length ? pickTopNFromHmacs(item.hmacs, item.winnersCount) : pickRandom(item.entries || [], item.winnersCount);
    saveGiveaway(gw);
    try {
      const ch = await client.channels.fetch(gw.channelId).catch(() => null);
      if (ch) {
        const msg = await ch.messages.fetch(gw.messageId).catch(() => null);
        if (msg) await msg.edit({ embeds: [buildGiveawayEmbed(gw)], components: buildButtonRowsForGiveaway(gw) }).catch(()=>{});
      }
    } catch (e) {}
    try {
      const ch = await client.channels.fetch(gw.channelId).catch(() => null);
      if (ch) { const announce = item.winners && item.winners.length ? `🔁 Reroll — **${item.prize}** — Winners: ${item.winners.map(id => `<@${id}>`).join(', ')}` : `🔁 Reroll — **${item.prize}** — No valid entries`; await ch.send({ content: announce }).catch(()=>{}); }
    } catch (e) {}
    return { status: 'rerolled_recompute', winners: item.winners };
  }

  if (!item.ended && !item.awaitingClientSeed) {
    item.endsAt = Date.now();
    saveGiveaway(gw);
    await endItem(gwId, itemId);
    return { status: 'force_ending' };
  }

  if (item.awaitingClientSeed) {
    if (typeof item.clientBlockNumTarget === 'number') {
      const block = await getBlockByNumber(item.clientBlockNumTarget);
      if (block && (block.blockID || (block.block_header && block.block_header.raw_data))) {
        await finalizeItemWithFetchedBlock(gw, item, { blockID: block.blockID || null, blockNum: item.clientBlockNumTarget, raw: block });
        return { status: 'finalized_immediately' };
      } else {
        await enqueueAwaitingItem(gwId, itemId);
        return { status: 'enqueued' };
      }
    } else {
      await enqueueAwaitingItem(gwId, itemId);
      return { status: 'enqueued' };
    }
  }

  return { status: 'no_action' };
}

// --- Express app ---
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
// ensure root serves index
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// --- Simple dashboard auth ---
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD || (Math.random().toString(36).slice(2, 10));
console.log('=== Dashboard password (one-time):', DASHBOARD_PASSWORD, '===');
const validDashboardTokens = new Map();

app.post('/api/auth/verify', (req, res) => {
  try {
    const pw = req.body && req.body.password ? String(req.body.password) : '';
    if (!pw) return res.status(400).json({ ok: false, error: 'missing_password' });
    if (pw === DASHBOARD_PASSWORD) {
      const token = crypto.randomBytes(24).toString('hex');
      validDashboardTokens.set(token, Date.now());
      return res.json({ ok: true, token });
    } else {
      return res.status(401).json({ ok: false, error: 'invalid' });
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'internal' });
  }
});
app.post('/api/auth/validate', (req, res) => {
  try {
    const token = req.headers['x-dashboard-token'] || (req.body && req.body.token);
    if (!token) return res.status(400).json({ ok: false, error: 'missing_token' });
    const ok = validDashboardTokens.has(String(token));
    return res.json({ ok });
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'internal' });
  }
});

// helper: dashboard token check for protected endpoints (non-admin)
function checkDashboardToken(req, res) {
  const token = req.headers['x-dashboard-token'];
  if (!token) return res.status(401).json({ error: 'missing_token' });
  if (!validDashboardTokens.has(String(token))) return res.status(403).json({ error: 'invalid_token' });
  return true;
}

// helper: admin check via ADMIN_SECRET header or dashboard token (for convenience)
function checkAdmin(req, res) {
  if (ADMIN_SECRET) {
    const provided = req.headers['x-admin-secret'];
    if (!provided || provided !== ADMIN_SECRET) {
      return res.status(403).json({ error: 'forbidden' });
    }
  } else {
    // fallback: require dashboard token
    if (!checkDashboardToken(req, res)) return false;
  }
  return true;
}

// Status & dashboard endpoints
app.get('/api/status', (req, res) => {
  try {
    const data = readData();
    const guilds = client && client.guilds ? client.guilds.cache.map(g => ({ id: g.id, name: g.name })) : [];
    res.json({ ok: true, ready: !!client && !!client.readyAt, botTag: client.user ? client.user.tag : null, botId: client.user ? client.user.id : null, guilds, requiredJoinRoleByGuild: data.requiredJoinRoleByGuild || {} });
  } catch (err) {
    res.status(500).json({ error: 'internal', detail: err.message });
  }
});

app.get('/api/giveaways', (req, res) => {
  try {
    if (!checkDashboardToken(req, res)) return;
    res.json({ giveaways: listGiveaways() });
  } catch (err) {
    res.status(500).json({ error: 'internal', detail: err.message });
  }
});

app.get('/api/guilds/:guildId/channels', async (req, res) => {
  try {
    if (!checkDashboardToken(req, res)) return;
    if (!client || !client.readyAt) return res.status(503).json({ error: 'bot_not_ready' });
    const guildId = req.params.guildId;
    const guild = await client.guilds.fetch(guildId).catch(() => null);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });
    const channels = await guild.channels.fetch().catch(() => guild.channels.cache);
    const out = [];
    for (const ch of channels.values()) {
      let isText = false;
      try { isText = typeof ch.isTextBased === 'function' ? ch.isTextBased() : (ch.type === ChannelType.GuildText || ch.type === ChannelType.GuildAnnouncement); } catch (e) { isText = false; }
      if (!isText) continue;
      let canSend = false;
      try { const me = await guild.members.fetch(client.user.id).catch(() => null); if (me) canSend = me.permissionsIn(ch).has(PermissionFlagsBits.SendMessages); } catch (e) { canSend = false; }
      out.push({ id: ch.id, name: ch.name || ch.id, type: ch.type, canSend });
    }
    res.json({ channels: out });
  } catch (err) {
    console.error('/api/guilds/:guildId/channels error', err && (err.stack || err.message || err));
    res.status(500).json({ error: 'internal', detail: err.message });
  }
});

app.get('/api/guilds/:guildId/roles', async (req, res) => {
  try {
    if (!checkDashboardToken(req, res)) return;
    if (!client || !client.readyAt) return res.status(503).json({ error: 'bot_not_ready' });
    const guildId = req.params.guildId;
    const guild = await client.guilds.fetch(guildId).catch(() => null);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });
    const roles = await guild.roles.fetch().catch(() => guild.roles.cache);
    const out = [];
    for (const r of roles.values()) out.push({ id: r.id, name: r.name, position: r.position, hoist: !!r.hoist });
    out.sort((a, b) => b.position - a.position);
    res.json({ roles: out });
  } catch (err) {
    console.error('/api/guilds/:guildId/roles error', err && (err.stack || err.message || err));
    res.status(500).json({ error: 'internal', detail: err.message });
  }
});

// Create giveaway
app.post('/api/giveaways', async (req, res) => {
  try {
    if (!checkDashboardToken(req, res)) return;
    const { guildId, channelId, hostId, isAllInOne, items } = req.body;
    if (!guildId || !channelId || !items || !Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'missing_fields' });
    if (!client || !client.readyAt) return res.status(503).json({ error: 'bot_not_ready' });

    const guild = await client.guilds.fetch(guildId).catch(() => null);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });
    const ch = await guild.channels.fetch(channelId).catch(() => null);
    if (!ch) return res.status(404).json({ error: 'channel_not_found' });

    const prepared = items.map((it, idx) => {
      const endsAt = it.endsAt ? Number(it.endsAt) : (Date.now() + 5 * 60000);
      return {
        id: `item_${Date.now()}_${idx}_${Math.floor(Math.random() * 9000 + 1000)}`,
        prize: it.prize || `(no prize ${idx})`,
        endsAt,
        winnersCount: Number(it.winnersCount || 1),
        requiredRole: it.requiredRole || null,
        entries: [],
        hmacs: {},
        winners: [],
        ended: false,
        clientBlockID: null,
        clientBlockNum: null,
        clientSeed: null,
        clientBlockNumTarget: null,
        awaitingClientSeed: false
      };
    });

    const gw = newGiveawayObj({ guildId, channelId, hostId, isAllInOne, items: prepared });

    const embed = buildGiveawayEmbed(gw);
    const tempRows = [new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('tmp_join').setLabel('Join').setStyle(ButtonStyle.Success))];
    const sent = await ch.send({ embeds: [embed], components: tempRows }).catch(err => { throw err; });
    gw.messageId = sent.id;
    const rows = buildButtonRowsForGiveaway(gw);
    await sent.edit({ components: rows }).catch(() => { });

    saveGiveaway(gw);
    for (const it of gw.items) scheduleItemEnd(gw.id, it.id);

    res.json({ ok: true, giveaway: gw });
  } catch (err) {
    console.error('POST /api/giveaways error', err && (err.stack || err.message || err));
    return res.status(500).json({ error: 'internal', detail: err && (err.message || err) });
  }
});

// Reroll
app.post('/api/giveaways/:gwId/reroll', async (req, res) => {
  try {
    if (!checkAdmin(req, res)) return;
    const gwId = req.params.gwId;
    const itemId = req.body.itemId;
    const reset = !!req.body.reset;
    if (!itemId) return res.status(400).json({ error: 'missing_itemId' });
    const out = await rerollItemHandler(gwId, itemId, reset);
    return res.json({ ok: true, result: out });
  } catch (err) {
    console.error('/api/giveaways/:gwId/reroll error', err && (err.stack || err.message || err));
    return res.status(500).json({ error: 'internal', detail: err && (err.message || err) });
  }
});

// Force end
app.post('/api/giveaways/:gwId/force-end', async (req, res) => {
  try {
    if (!checkAdmin(req, res)) return;
    const gwId = req.params.gwId;
    const gw = findGiveawayById(gwId);
    if (!gw) return res.status(404).json({ error: 'not_found' });
    const results = [];
    for (const it of gw.items) {
      if (!it.ended) {
        it.endsAt = Date.now();
        saveGiveaway(gw);
        await endItem(gwId, it.id);
        results.push({ itemId: it.id, status: 'ending' });
      } else results.push({ itemId: it.id, status: 'already_ended' });
    }
    res.json({ ok: true, result: results });
  } catch (err) {
    console.error('/api/giveaways/:gwId/force-end error', err && (err.stack || err.message || err));
    res.status(500).json({ error: 'internal', detail: err && (err.message || err) });
  }
});

// Delete
app.delete('/api/giveaways/:gwId', async (req, res) => {
  try {
    if (!checkAdmin(req, res)) return;
    const gwId = req.params.gwId;
    const gw = findGiveawayById(gwId);
    if (!gw) return res.status(404).json({ error: 'not_found' });

    try {
      const ch = await client.channels.fetch(gw.channelId).catch(() => null);
      if (ch) {
        const msg = await ch.messages.fetch(gw.messageId).catch(() => null);
        if (msg) await msg.delete().catch(() => { });
      }
    } catch (e) { /* ignore */ }

    removeGiveawayById(gwId);
    res.json({ ok: true });
  } catch (err) {
    console.error('DELETE /api/giveaways/:gwId error', err && (err.stack || err.message || err));
    res.status(500).json({ error: 'internal', detail: err && (err.message || err) });
  }
});

// start express
app.listen(PORT, () => console.log(`Dashboard/API running at http://localhost:${PORT}`));

// --- Discord client & interactions ---
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers],
  partials: [Partials.Message, Partials.Channel, Partials.Reaction]
});

// crash handlers -> exit so supervisor restarts
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION - exiting:', err && (err.stack || err.message || err));
  setTimeout(() => process.exit(1), 200);
});
process.on('unhandledRejection', (reason, p) => {
  console.error('UNHANDLED REJECTION - exiting. promise:', p, 'reason:', reason && (reason.stack || reason));
  setTimeout(() => process.exit(1), 200);
});
client.on('error', (err) => {
  console.error('Discord client error - exiting:', err && (err.stack || err.message || err));
  setTimeout(() => process.exit(1), 200);
});
client.on('shardError', (err) => {
  console.error('Discord shard error - exiting:', err && (err.stack || err.message || err));
  setTimeout(() => process.exit(1), 200);
});

// embed builder (relative timestamp only)
function buildGiveawayEmbed(gw) {
  const color = (() => {
    const anyAwaiting = gw.items.some(it => it.awaitingClientSeed && !it.ended);
    const allEnded = gw.items.every(it => it.ended);
    if (anyAwaiting) return COLOR_FETCHING;
    if (allEnded) return COLOR_ENDED;
    return COLOR_COUNTDOWN;
  })();

  const embed = new EmbedBuilder()
    .setTitle('🎉 GIVEAWAY')
    .setColor(color)
    .setDescription(`${gw.hostId ? `Hosted by: <@${gw.hostId}>\n\n` : ''}Server Public Key:\n\`${gw.serverPublicKey}\``);

  for (const it of gw.items) {
    let status;
    if (it.ended) status = `ENDED • Winners: ${it.winners.length}`;
    else if (it.awaitingClientSeed) status = it.clientBlockNumTarget ? `Awaiting block: ${it.clientBlockNumTarget} • Entries: ${it.entries.length}` : `Awaiting block (target unknown) • Entries: ${it.entries.length}`;
    else status = it.endsAt ? `Ends: <t:${Math.floor(it.endsAt / 1000)}:R> • Entries: ${it.entries.length}` : `Ends: (N/A) • Entries: ${it.entries.length}`;

    if (it.clientBlockNum && it.clientBlockID) {
      status += `\nClient Block: ${it.clientBlockNum}`;
      status += `\nClientSeed: \`${it.clientSeed}\``;
    }

    embed.addFields({ name: it.prize, value: (status || '').slice(0, 900), inline: false });
  }

  embed.setFooter({ text: `Giveaway id: ${gw.id}` });
  return embed;
}

function buildButtonRowsForGiveaway(gw) {
  const joinBtn = new ButtonBuilder().setCustomId(`gw_join:${gw.id}`).setLabel('Join').setStyle(ButtonStyle.Success);
  const verifyBtn = new ButtonBuilder().setCustomId(`gw_verify:${gw.id}`).setLabel('Verify').setStyle(ButtonStyle.Primary);
  const rows = [new ActionRowBuilder().addComponents(joinBtn), new ActionRowBuilder().addComponents(verifyBtn)];
  return rows;
}

// interactions: join / verify / details
client.on('interactionCreate', async (interaction) => {
  try {
    if (!interaction.isButton()) return;
    const cid = interaction.customId || '';

    // JOIN
    if (cid.startsWith('gw_join:')) {
      const gwId = cid.split(':')[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found', ephemeral: true });
      const userId = interaction.user.id;
      const joined = [], already = [], denied = [];

      const data = readData();
      const guildReq = (data.requiredJoinRoleByGuild || {})[gw.guildId] || null;

      let guildObj = null;
      try { guildObj = await client.guilds.fetch(gw.guildId); } catch (e) { guildObj = null; }

      for (const it of gw.items) {
        if (it.ended || it.awaitingClientSeed) continue;
        if (!it.entries) it.entries = [];
        if (it.entries.includes(userId)) { already.push(it.prize); continue; }
        const required = it.requiredRole || guildReq || null;
        if (required) {
          let has = false;
          if (guildObj) {
            const member = await guildObj.members.fetch(userId).catch(() => null);
            if (member && member.roles.cache.has(required)) has = true;
          }
          if (!has) { denied.push(it.prize); continue; }
        }
        it.entries.push(userId);
        joined.push(it.prize);
      }

      saveGiveaway(gw);

      try {
        const ch = await client.channels.fetch(gw.channelId).catch(() => null);
        if (ch) {
          const msg = await ch.messages.fetch(gw.messageId).catch(() => null);
          if (msg) await msg.edit({ embeds: [buildGiveawayEmbed(gw)], components: buildButtonRowsForGiveaway(gw) }).catch(() => { });
        }
      } catch (e) { /* ignore */ }

      const lines = [];
      if (joined.length) lines.push(`You have joined ${joined.map(s => `"${s}"`).join(', ')}.`);
      if (already.length) lines.push(`Already joined: ${already.map(s => `"${s}"`).join(', ')}.`);
      if (denied.length) lines.push(`Entries denied (missing role): ${denied.map(s => `"${s}"`).join(', ')}.`);
      if (!lines.length) lines.push('No eligible items to join.');

      return interaction.reply({ content: lines.join('\n'), ephemeral: true });
    }

    // VERIFY
    if (cid.startsWith('gw_verify:')) {
      const gwId = cid.split(':')[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found', ephemeral: true });

      const awaiting = gw.items.filter(it => it.awaitingClientSeed && !it.ended).map(it => it.prize);
      if (awaiting.length) return interaction.reply({ content: `Verification pending. Waiting for block seed for: ${awaiting.join(', ')}.`, ephemeral: true });

      const color = (() => {
        const anyAwaiting = gw.items.some(it => it.awaitingClientSeed && !it.ended);
        const allEnded = gw.items.every(it => it.ended);
        if (anyAwaiting) return COLOR_FETCHING;
        if (allEnded) return COLOR_ENDED;
        return COLOR_COUNTDOWN;
      })();

      const pfEmbed = new EmbedBuilder().setTitle('🔍 Provably-Fair (Summary)').setColor(color)
        .setDescription(`Giveaway: ${gw.id}\nServer Public Key:\n\`${gw.serverPublicKey}\``);

      for (const it of gw.items) {
        const lines = [];
        lines.push(`Prize: ${it.prize}`);
        lines.push(`Winners: ${it.winnersCount} • Entries: ${it.entries.length} • Ended: ${it.ended ? 'Yes' : 'No'}`);
        if (it.clientBlockNum && it.clientBlockID) {
          lines.push(`Client Block: ${it.clientBlockNum}`);
          lines.push(`ClientSeed: \`${it.clientSeed}\``);
        } else if (it.clientBlockNumTarget) {
          lines.push(`Client Block Target: ${it.clientBlockNumTarget} (awaiting block)`);
        } else {
          lines.push(`Client Block: (unavailable)`);
        }

        const arr = Object.entries(it.hmacs || {}).map(([uid, info]) => ({ uid, float: info.float })).sort((a, b) => b.float - a.float);
        if (arr.length) {
          const top = arr.slice(0, 10).map((t, i) => `${i + 1}. <@${t.uid}> — ${t.float.toFixed(12)} ${it.winners.includes(t.uid) ? '⭐' : ''}`);
          lines.push('Top entrants:\n' + top.join('\n'));
        } else {
          if (it.entries && it.entries.length) {
            lines.push(`Entrants (${it.entries.length}): ${it.entries.slice(0, 10).map(id => `<@${id}>`).join(', ')}${it.entries.length > 10 ? ', ...' : ''}`);
          } else {
            lines.push('(no entrants)');
          }
        }

        pfEmbed.addFields({ name: it.prize, value: lines.join('\n').slice(0, 900), inline: false });
      }

      const detailsRow = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId(`gw_details:${gw.id}`).setLabel('Details report').setStyle(ButtonStyle.Secondary));
      await interaction.reply({ embeds: [pfEmbed], components: [detailsRow], ephemeral: true });
      return;
    }

    // DETAILS
    if (cid.startsWith('gw_details:')) {
      const gwId = cid.split(':')[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found', ephemeral: true });

      const lines = [];
      for (const it of gw.items) {
        lines.push(`=== Prize: ${it.prize} ===`);
        lines.push(`Winners: ${it.winnersCount} • Entries: ${it.entries.length} • Ended: ${it.ended}`);
        if (it.clientBlockNum && it.clientBlockID) {
          lines.push(`Client Block: ${it.clientBlockNum}`);
          lines.push(`ClientSeed: ${it.clientSeed}`);
        } else if (it.clientBlockNumTarget) {
          lines.push(`Client Block Target: ${it.clientBlockNumTarget} (awaiting block)`);
        } else {
          lines.push('Client Block: (unavailable)');
        }

        if (it.hmacs && Object.keys(it.hmacs).length) {
          const arr = Object.entries(it.hmacs).map(([uid, info]) => ({ uid, float: info.float, hmac: info.hmac })).sort((a, b) => b.float - a.float);
          lines.push('Entrants (sorted by float):');
          for (let i = 0; i < arr.length; i++) {
            const row = arr[i];
            lines.push(`${i + 1}. <@${row.uid}> — ${row.float.toFixed(12)} — HMAC: ${row.hmac}`);
          }
        } else if (it.entries && it.entries.length) {
          lines.push('Entrants: ' + it.entries.map(e => `<@${e}>`).join(', '));
        } else {
          lines.push('(no entrants)');
        }

        if (it.winners && it.winners.length) lines.push('Winners: ' + it.winners.map(id => `<@${id}>`).join(', '));
        lines.push('');
      }

      const fullText = lines.join('\n');
      if (fullText.length <= 1900) {
        await interaction.reply({ content: 'Detailed report:\n```\n' + fullText + '\n```', ephemeral: true });
      } else {
        const buffer = Buffer.from(fullText, 'utf8');
        await interaction.reply({ files: [{ attachment: buffer, name: `verify-${gw.id}.txt` }], ephemeral: true });
      }
      return;
    }

  } catch (err) {
    console.error('interactionCreate error', err && (err.stack || err.message || err));
    try { if (interaction && !interaction.replied) interaction.reply({ content: 'Internal error', ephemeral: true }); } catch (_) {}
  }
});

// ready: reschedule timers & start worker
client.once('ready', () => {
  console.log('Bot ready:', client.user.tag);
  const gws = listGiveaways();
  for (const gw of gws) {
    for (const it of gw.items) {
      if (!it.ended && it.endsAt) scheduleItemEnd(gw.id, it.id);
    }
  }
  processAwaitingQueue().catch(err => console.error('processAwaitingQueue startup error', err));
});

// login
if (!process.env.BOT_TOKEN) {
  console.error('Missing BOT_TOKEN in .env');
  process.exit(1);
}
client.login(process.env.BOT_TOKEN).catch(err => { console.error('Login failed', err && (err.stack || err.message || err)); process.exit(1); });

