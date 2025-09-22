// index.js
// Updated: Verify shows PF embed + "Details report" button. Details button shows full data (ephemeral).
// Embed colors depend on giveaway state: countdown (blue), fetching (amber), ended (green).
// All previous PF behavior preserved: target block = now + 2, single-worker queue, 10s polling, persistence.

require('dotenv').config();
const fs = require('fs-extra');
const path = require('path');
const express = require('express');
const crypto = require('crypto');

let fetchFunc = globalThis.fetch;
if (!fetchFunc) {
  try { fetchFunc = require('node-fetch'); } catch (e) { console.warn('fetch unavailable; TRON API may fail on Node <18'); }
}

const {
  Client,
  GatewayIntentBits,
  Partials,
  PermissionFlagsBits,
  ChannelType,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle
} = require('discord.js');

const DATA_PATH = path.join(__dirname, 'pf_data.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const PORT = Number(process.env.PORT || 3000);
const TRON_GRID_API = process.env.TRON_GRID_API || 'https://api.trongrid.io';
const TRONSCAN_BLOCK_URL = 'https://tronscan.org/#/block/';

// embed colors
const COLOR_COUNTDOWN = 0x2563eb; // blue
const COLOR_FETCHING  = 0xf59e0b; // amber
const COLOR_ENDED     = 0x16a34a; // green

// ---------- persistence ----------
const DEFAULT_DATA = { requiredJoinRoleByGuild: {}, guilds: {}, giveaways: [] };
function readData(){
  try {
    if (!fs.existsSync(DATA_PATH)) {
      fs.writeFileSync(DATA_PATH, JSON.stringify(DEFAULT_DATA, null, 2));
      return JSON.parse(JSON.stringify(DEFAULT_DATA));
    }
    const raw = fs.readFileSync(DATA_PATH, 'utf8') || '{}';
    const obj = JSON.parse(raw);
    obj.requiredJoinRoleByGuild = obj.requiredJoinRoleByGuild || {};
    obj.guilds = obj.guilds || {};
    obj.giveaways = Array.isArray(obj.giveaways) ? obj.giveaways : [];
    obj.giveaways = obj.giveaways.map(g => {
      if (!g) return null;
      g.items = Array.isArray(g.items) ? g.items.map(it => ({
        id: it.id,
        prize: it.prize || '(no prize)',
        endsAt: Number(it.endsAt || 0),
        winnersCount: Number(it.winnersCount || 1),
        requiredRole: it.requiredRole || null,
        entries: Array.isArray(it.entries) ? it.entries : [],
        hmacs: (it.hmacs && typeof it.hmacs === 'object') ? it.hmacs : {},
        winners: Array.isArray(it.winners) ? it.winners : [],
        ended: !!it.ended,
        clientBlockID: it.clientBlockID || null,
        clientBlockNum: it.clientBlockNum || null,
        clientSeed: it.clientSeed || null,
        clientBlockNumTarget: it.clientBlockNumTarget || null,
        awaitingClientSeed: !!it.awaitingClientSeed
      })) : [];
      return g;
    }).filter(Boolean);
    return obj;
  } catch (err) {
    console.error('readData failed, recreating:', err.message);
    fs.writeFileSync(DATA_PATH, JSON.stringify(DEFAULT_DATA, null, 2));
    return JSON.parse(JSON.stringify(DEFAULT_DATA));
  }
}
function writeData(obj){
  const tmp = DATA_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, DATA_PATH);
}

// ---------- utilities ----------
function random64(){
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i=0;i<64;i++) s += chars[Math.floor(Math.random()*chars.length)];
  return s;
}
function hmacSha512Hex(key, msg){ return crypto.createHmac('sha512', key).update(msg).digest('hex'); }
function hmacHexToFloat(hex){ const first = hex.slice(0,13); const num = parseInt(first,16); return num / Math.pow(16, first.length); }
function delay(ms){ return new Promise(res=>setTimeout(res, ms)); }

// ---------- TRON helpers ----------
async function getNowBlockRaw(){
  try {
    if (!fetchFunc) throw new Error('fetch unavailable');
    const url = TRON_GRID_API.replace(/\/$/, '') + '/wallet/getnowblock';
    const r = await fetchFunc(url);
    if (!r || !r.ok) throw new Error('getnowblock status ' + (r && r.status));
    return await r.json();
  } catch (e) {
    console.warn('getNowBlockRaw failed:', e.message);
    return null;
  }
}
function extractBlockNumber(blockObj){
  if (!blockObj) return null;
  try {
    if (blockObj.block_header && blockObj.block_header.raw_data && typeof blockObj.block_header.raw_data.number === 'number') return blockObj.block_header.raw_data.number;
    if (typeof blockObj.block_num === 'number') return blockObj.block_num;
    if (typeof blockObj.number === 'number') return blockObj.number;
  } catch(e){}
  return null;
}
async function getBlockByNumber(num){
  try {
    if (!fetchFunc) throw new Error('fetch unavailable');
    const url = TRON_GRID_API.replace(/\/$/, '') + '/wallet/getblockbynum';
    const r = await fetchFunc(url, { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify({ num }) });
    if (!r || !r.ok) throw new Error('getblockbynum status ' + (r && r.status));
    return await r.json();
  } catch(e) {
    console.warn('getBlockByNumber failed for', num, e.message);
    return null;
  }
}

// ---------- giveaways model & scheduling ----------
function newGiveawayObj({ guildId, channelId, hostId, items, isAllInOne }) {
  return {
    id: 'gw_' + Date.now() + '_' + Math.floor(Math.random()*9999),
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
      endsAt: Number(it.endsAt),
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
    }))
  };
}
function saveGiveaway(gw){
  const d = readData();
  d.giveaways = d.giveaways || [];
  const idx = d.giveaways.findIndex(x => x.id === gw.id);
  if (idx === -1) d.giveaways.push(gw); else d.giveaways[idx] = gw;
  writeData(d);
}
function findGiveawayById(gwId){ return (readData().giveaways||[]).find(g => g.id === gwId); }
function findGiveawayByMessage(guildId, messageId){ return (readData().giveaways||[]).find(g => g.guildId === guildId && g.messageId === messageId); }
function listGiveaways(){ return readData().giveaways || []; }

const itemTimers = new Map();

// ---------- single-worker queue for awaiting items ----------
let processingAwaitingQueue = false;

async function enqueueAwaitingItem(gwId, itemId){
  const gw = findGiveawayById(gwId);
  if (!gw) return;
  const item = gw.items.find(it=>it.id === itemId);
  if (!item || item.ended) return;
  item.awaitingClientSeed = true;
  saveGiveaway(gw);
  processAwaitingQueue().catch(err=>console.error('processAwaitingQueue error', err));
}

async function getNextAwaitingItem(){
  const d = readData();
  let best = null;
  for (const gw of d.giveaways || []) {
    for (const it of gw.items || []) {
      if (it.awaitingClientSeed && !it.ended) {
        if (!best || (it.clientBlockNumTarget || it.endsAt || 1e18) < (best.item.clientBlockNumTarget || best.item.endsAt || 1e18)) best = { gw, item: it };
      }
    }
  }
  return best;
}

async function processAwaitingQueue(){
  if (processingAwaitingQueue) return;
  processingAwaitingQueue = true;
  try {
    while (true) {
      const next = await getNextAwaitingItem();
      if (!next) break;
      const gw = findGiveawayById(next.gw.id);
      if (!gw) continue;
      const item = gw.items.find(it => it.id === next.item.id);
      if (!item || item.ended || !item.awaitingClientSeed) continue;

      const targetNum = item.clientBlockNumTarget;
      console.log(`Worker: polling for target block ${targetNum || '(no numeric target)'} for item ${item.id} (giveaway ${gw.id}). Poll interval 10s.`);

      let success = false;
      while (!success) {
        try {
          let block = null;
          if (typeof targetNum === 'number') {
            block = await getBlockByNumber(targetNum);
          } else {
            const nowraw = await getNowBlockRaw();
            const nowNum = extractBlockNumber(nowraw);
            if (nowNum !== null) {
              const t = nowNum + 2;
              block = await getBlockByNumber(t);
            } else {
              block = null;
            }
          }

          if (block && (block.blockID || (block.block_header && block.block_header.raw_data))) {
            const blockID = block.blockID || (block.block_header && block.block_header.raw_data && block.block_header.raw_data.txTrieRoot) || null;
            const blockNum = typeof targetNum === 'number' ? targetNum : (extractBlockNumber(block) || null);
            await finalizeItemWithFetchedBlock(gw, item, { blockID, blockNum, raw: block });
            success = true;
            break;
          } else {
            await delay(10000);
            const freshGw = findGiveawayById(gw.id);
            if (!freshGw) { success = true; break; }
            const freshItem = freshGw.items.find(it=>it.id===item.id);
            if (!freshItem || freshItem.ended || !freshItem.awaitingClientSeed) { success = true; break; }
          }
        } catch (err) {
          console.warn('Polling error for awaiting item:', err.message || err);
          await delay(10000);
        }
      }
    }
  } finally {
    processingAwaitingQueue = false;
  }
}

// ---------- finalize helpers ----------
function pickFromMap(hmap, n){
  const arr = Object.entries(hmap || {}).map(([uid,info]) => ({ uid, float: info.float }));
  arr.sort((a,b)=> b.float - a.float);
  return arr.slice(0,n).map(x=>x.uid);
}
function pickRandom(entries, n){
  const uniq = Array.from(new Set(entries || []));
  const winners = [];
  while(winners.length < n && uniq.length>0) winners.push(uniq.splice(Math.floor(Math.random()*uniq.length),1)[0]);
  return winners;
}

async function finalizeItemWithFetchedBlock(gw, item, target){
  try {
    const gwFresh = findGiveawayById(gw.id) || gw;
    const itemFresh = gwFresh.items.find(it=>it.id===item.id) || item;

    itemFresh.clientBlockID = target && target.blockID ? target.blockID : itemFresh.clientBlockID || null;
    itemFresh.clientBlockNum = typeof target.blockNum === 'number' ? target.blockNum : itemFresh.clientBlockNum || null;
    itemFresh.clientSeed = itemFresh.clientBlockID || '(unavailable)';

    itemFresh.hmacs = itemFresh.hmacs || {};
    for (const uid of itemFresh.entries || []) {
      try {
        const h = hmacSha512Hex(gwFresh.serverPublicKey || '', `${itemFresh.clientSeed}:${uid}`);
        const f = hmacHexToFloat(h);
        itemFresh.hmacs[uid] = { hmac: h, float: f };
      } catch(e) { console.warn('hmac compute error', e.message); }
    }

    itemFresh.winners = Object.keys(itemFresh.hmacs).length ? pickFromMap(itemFresh.hmacs, itemFresh.winnersCount) : pickRandom(itemFresh.entries || [], itemFresh.winnersCount);
    itemFresh.ended = true;
    itemFresh.awaitingClientSeed = false;
    saveGiveaway(gwFresh);

    try {
      const ch = await client.channels.fetch(gwFresh.channelId).catch(()=>null);
      if (ch) {
        const msg = await ch.messages.fetch(gwFresh.messageId).catch(()=>null);
        if (msg) {
          const embed = buildGiveawayEmbed(gwFresh);
          const rows = buildButtonRowsForGiveaway(gwFresh);
          await msg.edit({ embeds: [embed], components: rows }).catch(()=>{});
        }
      }
    } catch(e){ console.warn('update message after finalize failed', e.message); }

    // announce winners
    try {
      const ch = await client.channels.fetch(gwFresh.channelId).catch(()=>null);
      if (ch) {
        const announce = itemFresh.winners && itemFresh.winners.length ? `🎉 **${itemFresh.prize}** — Winners: ${itemFresh.winners.map(id=>`<@${id}>`).join(', ')}` : `🎁 **${itemFresh.prize}** — No valid entries`;
        await ch.send({ content: announce }).catch(()=>{});
      }
    } catch(e){}

    console.log(`Finalized item ${itemFresh.id} in giveaway ${gwFresh.id}, winners: ${itemFresh.winners.length}`);
  } catch (err) {
    console.error('finalizeItemWithFetchedBlock error', err);
  }
}

// ---------- endItem ----------
async function endItem(gwId, itemId){
  try {
    const gw = findGiveawayById(gwId);
    if (!gw) { console.warn('endItem: giveaway not found', gwId); return; }
    const item = gw.items.find(it=>it.id === itemId);
    if (!item) { console.warn('endItem: item not found', gwId, itemId); return; }
    if (item.ended) return;

    const nowRaw = await getNowBlockRaw();
    const nowNum = extractBlockNumber(nowRaw);
    if (nowNum === null) {
      console.warn('endItem: could not get current block number; marking awaiting without numeric target.');
      item.awaitingClientSeed = true;
      item.clientBlockNumTarget = null;
      saveGiveaway(gw);
      await enqueueAwaitingItem(gwId, itemId);
      try {
        const ch = await client.channels.fetch(gw.channelId).catch(()=>null);
        if (ch) {
          const msg = await ch.messages.fetch(gw.messageId).catch(()=>null);
          if (msg) {
            const embed = buildGiveawayEmbed(gw);
            const rows = buildButtonRowsForGiveaway(gw);
            await msg.edit({ embeds: [embed], components: rows }).catch(()=>{});
          }
        }
      } catch(e){}
      return;
    }

    const targetNum = nowNum + 2;
    item.clientBlockNumTarget = targetNum;
    item.awaitingClientSeed = true;
    saveGiveaway(gw);

    try {
      const ch = await client.channels.fetch(gw.channelId).catch(()=>null);
      if (ch) {
        const msg = await ch.messages.fetch(gw.messageId).catch(()=>null);
        if (msg) {
          const embed = buildGiveawayEmbed(gw);
          const rows = buildButtonRowsForGiveaway(gw);
          await msg.edit({ embeds: [embed], components: rows }).catch(()=>{});
        }
      }
    } catch(e){}

    await enqueueAwaitingItem(gwId, itemId);

  } catch (err) {
    console.error('endItem error', err);
  } finally {
    const key = `${gwId}:${itemId}`;
    if (itemTimers.has(key)){ clearTimeout(itemTimers.get(key)); itemTimers.delete(key); }
  }
}

function scheduleItemEnd(gwId, itemId){
  const gw = findGiveawayById(gwId);
  if (!gw) return;
  const item = gw.items.find(it=>it.id === itemId);
  if (!item || item.ended) return;
  const key = `${gwId}:${itemId}`;
  if (itemTimers.has(key)) clearTimeout(itemTimers.get(key));
  const ms = item.endsAt - Date.now();
  if (ms <= 0) { setTimeout(()=> endItem(gwId, itemId).catch(()=>{}), 1000); return; }
  const t = setTimeout(()=> endItem(gwId, itemId).catch(err=>console.error(err)), ms);
  itemTimers.set(key, t);
}

// ---------- embed & buttons ----------
function determineGiveawayColor(gw){
  const anyAwaiting = gw.items.some(it => it.awaitingClientSeed && !it.ended);
  const allEnded = gw.items.every(it => it.ended);
  if (anyAwaiting) return COLOR_FETCHING;
  if (allEnded) return COLOR_ENDED;
  return COLOR_COUNTDOWN;
}

function buildGiveawayEmbed(gw){
  const color = determineGiveawayColor(gw);
  const embed = new EmbedBuilder().setTitle('🎉 GIVEAWAY').setColor(color)
    .setDescription(`${gw.hostId ? `Hosted by: <@${gw.hostId}>\n\n` : ''}Server Public Key (per-giveaway):\n\`${gw.serverPublicKey}\``)
    .setTimestamp();

  for (const it of gw.items) {
    let status;
    if (it.ended) {
      status = `ENDED • Winners: ${it.winners.length}`;
    } else if (it.awaitingClientSeed) {
      status = it.clientBlockNumTarget ? `Awaiting block seed (target block: ${it.clientBlockNumTarget}) • Entries: ${it.entries.length}` : `Awaiting block seed (target unknown) • Entries: ${it.entries.length}`;
    } else {
      status = `Ends: ${it.endsAt ? `<t:${Math.floor(it.endsAt/1000)}:F> (<t:${Math.floor(it.endsAt/1000)}:R>)` : 'N/A'} • Entries: ${it.entries.length}`;
    }
    if (it.clientBlockNum && it.clientBlockID) {
      status += `\nClient Block: [${it.clientBlockNum}](${TRONSCAN_BLOCK_URL + encodeURIComponent(it.clientBlockID)})`;
      status += `\nClientSeed: \`${it.clientSeed}\``;
    }
    let val = status;
    if (val.length > 900) val = val.slice(0, 890) + '...';
    embed.addFields({ name: it.prize, value: val, inline: false });
  }
  embed.setFooter({ text: `Giveaway id: ${gw.id}` });
  return embed;
}

function buildButtonRowsForGiveaway(gw){
  const rows = [];
  rows.push(new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId(`gw_join:${gw.id}:${gw.messageId}`).setLabel('Join').setStyle(ButtonStyle.Success)));
  rows.push(new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId(`gw_verify:${gw.id}:${gw.messageId}`).setLabel('Verify').setStyle(ButtonStyle.Primary)));
  return rows;
}

// ---------- Discord client & interactions ----------
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent, GatewayIntentBits.GuildMembers],
  partials: [Partials.Message, Partials.Channel]
});

function ensureUserRecord(guildId, uid){
  const d = readData();
  if (!d.guilds[guildId]) d.guilds[guildId] = { users: {} };
  if (!d.guilds[guildId].users[uid]) { d.guilds[guildId].users[uid] = { nonce: 0 }; writeData(d); }
  return d.guilds[guildId].users[uid];
}

client.on('interactionCreate', async (interaction) => {
  try {
    if (!interaction.isButton || !interaction.isButton()) return;
    const cid = interaction.customId || '';

    // Join
    if (cid.startsWith('gw_join:')) {
      const parts = cid.split(':');
      const gwId = parts[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found.', ephemeral: true });

      const userId = interaction.user.id;
      const joinedNow = [], alreadyJoined = [], denied = [];

      const d = readData();
      const guildReq = d.requiredJoinRoleByGuild && d.requiredJoinRoleByGuild[gw.guildId] ? d.requiredJoinRoleByGuild[gw.guildId] : null;
      let guildObj = null;
      try { guildObj = await safeGetGuild(gw.guildId); } catch(e){ guildObj = null; }

      for (const it of gw.items) {
        if (it.ended || it.awaitingClientSeed) continue;
        it.entries = it.entries || [];
        if (it.entries.includes(userId)) { alreadyJoined.push(it.prize); continue; }
        const required = it.requiredRole || guildReq || null;
        if (required) {
          let hasRole = false;
          if (guildObj) {
            const member = await guildObj.members.fetch(userId).catch(()=>null);
            if (member && member.roles.cache.has(required)) hasRole = true;
          }
          if (!hasRole) { denied.push(it.prize); continue; }
        }
        it.entries.push(userId);
        joinedNow.push(it.prize);
      }

      if (joinedNow.length > 0) saveGiveaway(gw);

      try {
        const ch = await client.channels.fetch(gw.channelId).catch(()=>null);
        if (ch) {
          const msg = await ch.messages.fetch(gw.messageId).catch(()=>null);
          if (msg) {
            const embed = buildGiveawayEmbed(gw);
            const rows = buildButtonRowsForGiveaway(gw);
            await msg.edit({ embeds: [embed], components: rows }).catch(()=>{});
          }
        }
      } catch(e){}

      function quoteList(arr) {
        if (!arr || arr.length === 0) return '';
        if (arr.length === 1) return `"${arr[0]}"`;
        if (arr.length === 2) return `"${arr[0]}" and "${arr[1]}"`;
        const allButLast = arr.slice(0,-1).map(x=>`"${x}"`).join(', ');
        const last = `"${arr[arr.length-1]}"`;
        return `${allButLast} and ${last}`;
      }

      if (joinedNow.length === 0 && denied.length === 0 && alreadyJoined.length > 0) {
        return interaction.reply({ content: `You have already joined ${quoteList(alreadyJoined)}.`, ephemeral: true });
      }

      const lines = [];
      if (joinedNow.length > 0) lines.push(`You joined ${quoteList(joinedNow)}.`);
      if (alreadyJoined.length > 0) lines.push(`Already joined before: ${quoteList(alreadyJoined)}.`);
      if (denied.length > 0) lines.push(`Could not join (missing role): ${quoteList(denied)}.`);
      if (lines.length === 0) lines.push('No eligible items to join.');

      return interaction.reply({ content: lines.join('\n'), ephemeral: true });
    }

    // Verify -> show PF embed summary + Details button (ephemeral)
    if (cid.startsWith('gw_verify:')) {
      const parts = cid.split(':');
      const gwId = parts[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found.', ephemeral: true });

      // If any item awaiting -> reply pending
      const awaiting = gw.items.filter(it => it.awaitingClientSeed && !it.ended).map(it => it.prize);
      if (awaiting.length > 0) {
        const list = awaiting.map(s => `"${s}"`).join(', ');
        return interaction.reply({ content: `Verification pending. Waiting for block seed for: ${list}. Please try again after all items are rolled.`, ephemeral: true });
      }

      // Build pf embed (short) and include a Details button in the reply
      const color = determineGiveawayColor(gw);
      const pfEmbed = new EmbedBuilder()
        .setTitle('🔍 Provably-Fair (Summary)')
        .setColor(color)
        .setDescription(`Giveaway: ${gw.id}\nServer Public Key:\n\`${gw.serverPublicKey}\``)
        .setTimestamp();

      const FIELD_MAX = 900;
      function chunkString(str, size){
        if (!str) return [''];
        const out = [];
        for (let i=0;i<str.length;i+=size) out.push(str.slice(i,i+size));
        return out;
      }

      // Build summary fields (entrants show as mentions)
      for (const it of gw.items) {
        const lines = [];
        lines.push(`Prize: ${it.prize}`);
        lines.push(`Winners: ${it.winnersCount} • Entries: ${it.entries.length} • Ended: ${it.ended}`);
        if (it.clientBlockNum && it.clientBlockID) {
          lines.push(`Client Block: [${it.clientBlockNum}](${TRONSCAN_BLOCK_URL + encodeURIComponent(it.clientBlockID)})`);
          lines.push(`ClientSeed: ${it.clientSeed}`);
        } else if (it.clientBlockNumTarget) {
          lines.push(`Client Block Target: ${it.clientBlockNumTarget} (awaiting block data)`);
        } else {
          lines.push(`Client Block: (unavailable)`);
        }

        // show top entrants as mentions (top 10)
        const arr = Object.entries(it.hmacs || {}).map(([uid,info]) => ({ uid, hmac: info.hmac, float: info.float })).sort((a,b)=>b.float-a.float);
        if (arr.length > 0) {
          lines.push('Top entrants (top 10 shown):');
          const top = arr.slice(0,10);
          for (let i=0;i<top.length;i++){
            const t = top[i];
            lines.push(`${i+1}. <@${t.uid}> — ${t.float.toFixed(12)} ${it.winners.includes(t.uid) ? '⭐' : ''}`);
          }
          if (arr.length > 10) lines.push(`...and ${arr.length-10} more (click Details report).`);
        } else {
          if (it.entries && it.entries.length) {
            const mentions = it.entries.slice(0,10).map(id=>`<@${id}>`).join(', ');
            lines.push(`Entrants (${it.entries.length}): ${mentions}${it.entries.length>10? ', ...': ''}`);
          } else lines.push('(no entrants)');
        }

        const blockText = lines.join('\n');
        if (blockText.length <= FIELD_MAX) pfEmbed.addFields({ name: it.prize, value: blockText, inline: false });
        else {
          const chunks = chunkString(blockText, FIELD_MAX);
          for (let i=0;i<chunks.length;i++){
            pfEmbed.addFields({ name: i===0 ? it.prize : `${it.prize} (cont ${i})`, value: chunks[i], inline: false });
          }
        }
      }

      // calculation short note
      pfEmbed.addFields({ name: 'Calculation', value: 'H = HMAC_SHA512(serverPublicKey, clientSeed:entrantId)\nfloat = parseInt(first13hex,16)/16^13\nSort floats descending; top N are winners.', inline: false });

      // build Details button
      const detailsRow = new ActionRowBuilder()
        .addComponents(new ButtonBuilder().setCustomId(`gw_details:${gw.id}:${gw.messageId}`).setLabel('Details report').setStyle(ButtonStyle.Secondary));

      await interaction.reply({ embeds: [pfEmbed], components: [detailsRow], ephemeral: true });
      return;
    }

    // Details report: send full details (ephemeral) with entrants as mentions and full HMACs/floats
    if (cid.startsWith('gw_details:')) {
      const parts = cid.split(':');
      const gwId = parts[1];
      const gw = findGiveawayById(gwId);
      if (!gw) return interaction.reply({ content: 'Giveaway not found.', ephemeral: true });

      // Build full report
      const fullLines = [];
      for (const it of gw.items) {
        fullLines.push(`=== Prize: ${it.prize} ===`);
        fullLines.push(`Winners: ${it.winnersCount} • Entries: ${it.entries.length} • Ended: ${it.ended}`);
        if (it.clientBlockNum && it.clientBlockID) {
          fullLines.push(`Client Block: [${it.clientBlockNum}](${TRONSCAN_BLOCK_URL + encodeURIComponent(it.clientBlockID)})`);
          fullLines.push(`ClientSeed: ${it.clientSeed}`);
        } else if (it.clientBlockNumTarget) {
          fullLines.push(`Client Block Target: ${it.clientBlockNumTarget} (awaiting block data)`);
        } else {
          fullLines.push(`Client Block: (unavailable)`);
        }

        if (it.hmacs && Object.keys(it.hmacs).length) {
          const arr = Object.entries(it.hmacs).map(([uid,info]) => ({ uid, hmac: info.hmac, float: info.float })).sort((a,b)=>b.float-a.float);
          fullLines.push('Full entrant list (sorted by float desc):');
          for (let i=0;i<arr.length;i++){
            const t = arr[i];
            fullLines.push(`${i+1}. <@${t.uid}> (${t.uid}) — ${t.float.toFixed(12)} — HMAC: ${t.hmac}`);
          }
        } else if (it.entries && it.entries.length) {
          fullLines.push('Entrants (no HMACs computed):');
          for (const e of it.entries) fullLines.push(`- <@${e}> (${e})`);
        } else {
          fullLines.push('(no entrants)');
        }

        if (it.winners && it.winners.length) {
          fullLines.push('Winners: ' + it.winners.map(id=>`<@${id}>`).join(', '));
        }
        fullLines.push('');
      }

      fullLines.push('How winners are calculated:');
      fullLines.push('H = HMAC_SHA512(serverPublicKey, clientSeed:entrantId)');
      fullLines.push('float = parseInt(first13hex,16)/16^13');
      fullLines.push('Sort floats descending; top N are winners.');
      const fullText = fullLines.join('\n');

      try {
        // if small, send as code block; else attach file
        if (fullText.length <= 1900) {
          await interaction.reply({ content: 'Detailed report:\n```' + fullText + '```', ephemeral: true });
        } else {
          const buffer = Buffer.from(fullText, 'utf8');
          await interaction.reply({ files: [{ attachment: buffer, name: `verify-${gw.id}.txt` }], ephemeral: true });
        }
      } catch (err) {
        console.error('gw_details reply failed', err);
        try { await interaction.reply({ content: 'Failed to send details report.', ephemeral: true }); } catch(_) {}
      }
      return;
    }

  } catch (err) {
    console.error('interactionCreate error', err);
    try { if (interaction && !interaction.replied) interaction.reply({ content: 'Internal error', ephemeral: true }); } catch(_) {}
  }
});

// ---------- Express API & UI ----------
const app = express();
app.use(express.static(PUBLIC_DIR));
app.use(express.json());

async function safeGetGuild(gid){
  if (!client || !client.isReady()) throw new Error('bot_not_ready');
  let guild = client.guilds.cache.get(gid);
  if (!guild) {
    try { guild = await client.guilds.fetch(gid); } catch(e) { return null; }
  }
  return guild;
}

app.get('/api/status', (req, res) => {
  const data = readData();
  const ready = client && client.isReady ? client.isReady() : false;
  const guilds = client && client.guilds ? client.guilds.cache.map(g => ({ id: g.id, name: g.name })) : [];
  res.json({ ready, botTag: client.user ? client.user.tag : null, botId: client.user ? client.user.id : null, requiredJoinRoleByGuild: data.requiredJoinRoleByGuild || {}, guilds });
});

app.get('/api/giveaways', (req, res) => {
  res.json({ giveaways: listGiveaways() });
});

app.get('/api/giveaway/:guildId/:messageId', (req, res) => {
  const { guildId, messageId } = req.params;
  const gw = findGiveawayByMessage(guildId, messageId);
  if (!gw) return res.status(404).json({ error: 'not_found' });
  return res.json({ giveaway: gw });
});

app.get('/api/guilds/:guildId/channels', async (req, res) => {
  const guildId = req.params.guildId;
  try {
    if (!client || !client.isReady()) return res.status(503).json({ error: 'bot_not_ready' });
    const guild = await safeGetGuild(guildId);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });

    let me;
    try { me = await guild.members.fetch(client.user.id); } catch(e) { me = guild.members.cache.get(client.user.id) || null; }
    let collection;
    try { collection = await guild.channels.fetch(); } catch(e) { collection = guild.channels.cache; }
    const out = [];
    for (const ch of collection.values()) {
      let isText = false;
      try { isText = (typeof ch.isTextBased === 'function') ? ch.isTextBased() : (ch.type === ChannelType.GuildText || ch.type === ChannelType.GuildAnnouncement); } catch(e) {}
      if (!isText) continue;
      let canSend = null;
      if (me) { try { canSend = me.permissionsIn(ch).has(PermissionFlagsBits.SendMessages); } catch(e) { canSend = false; } }
      out.push({ id: ch.id, name: ch.name || ch.id, type: ch.type, canSend });
    }
    return res.json({ channels: out });
  } catch(err) {
    console.error('/api/guilds/:guildId/channels error', err.message || err);
    if (err.message === 'bot_not_ready') return res.status(503).json({ error: 'bot_not_ready' });
    return res.status(500).json({ error: 'internal', detail: err.message });
  }
});

app.get('/api/guilds/:guildId/roles', async (req, res) => {
  const guildId = req.params.guildId;
  try {
    if (!client || !client.isReady()) return res.status(503).json({ error: 'bot_not_ready' });
    const guild = await safeGetGuild(guildId);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });
    let roles;
    try { roles = await guild.roles.fetch(); } catch(e) { roles = guild.roles.cache; }
    const out = [];
    for (const r of roles.values()) out.push({ id: r.id, name: r.name, position: r.position, hoist: !!r.hoist });
    out.sort((a,b)=> b.position - a.position);
    return res.json({ roles: out });
  } catch(err) {
    console.error('/api/guilds/:guildId/roles error', err.message || err);
    if (err.message === 'bot_not_ready') return res.status(503).json({ error: 'bot_not_ready' });
    return res.status(500).json({ error: 'internal', detail: err.message });
  }
});

// create giveaway
app.post('/api/giveaways', async (req, res) => {
  try {
    const { guildId, channelId, hostId, isAllInOne, items } = req.body;
    if (!guildId || !channelId || !items || !Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'missing_fields' });
    if (!client || !client.isReady()) return res.status(503).json({ error: 'bot_not_ready' });

    const guild = await safeGetGuild(guildId);
    if (!guild) return res.status(404).json({ error: 'guild_not_found' });
    const ch = await guild.channels.fetch(channelId).catch(()=>null);
    if (!ch) return res.status(404).json({ error: 'channel_not_found' });

    const me = await guild.members.fetch(client.user.id).catch(()=>null);
    if (!me || !me.permissionsIn(ch).has(PermissionFlagsBits.SendMessages)) return res.status(403).json({ error: 'no_permission_send' });

    const prepared = items.map((it, idx) => {
      const duration = Number(it.durationMinutes || 0);
      const endsAt = it.endsAt ? Number(it.endsAt) : (duration > 0 ? Date.now() + duration * 60000 : Date.now() + 5*60000);
      return {
        id: `item_${Date.now()}_${idx}_${Math.floor(Math.random()*9999)}`,
        prize: it.prize || `(no prize ${idx})`,
        endsAt,
        winnersCount: Number(it.winnersCount || 1),
        requiredRole: it.requiredRole || null
      };
    });

    const gw = newGiveawayObj({ guildId, channelId, hostId, items: prepared, isAllInOne: !!isAllInOne });

    const embed = buildGiveawayEmbed(gw);
    const tempRows = [];
    tempRows.push(new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('tmp_join').setLabel('Join').setStyle(ButtonStyle.Success)));
    tempRows.push(new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('tmp_verify').setLabel('Verify').setStyle(ButtonStyle.Primary)));
    const sent = await ch.send({ embeds: [embed], components: tempRows });
    gw.messageId = sent.id;
    const rows = buildButtonRowsForGiveaway(gw);
    await sent.edit({ components: rows }).catch(()=>{});

    saveGiveaway(gw);
    for (const it of gw.items) scheduleItemEnd(gw.id, it.id);

    return res.json({ ok: true, giveaway: gw });
  } catch (err) {
    console.error('POST /api/giveaways error', err);
    return res.status(500).json({ error: 'internal', detail: err.message });
  }
});

app.listen(PORT, ()=> console.log(`Dashboard running at http://localhost:${PORT}`));

// ---------- client ready ----------
client.once('ready', () => {
  console.log('Bot ready:', client.user.tag);
  const gws = listGiveaways();
  for (const gw of gws) {
    for (const it of gw.items) {
      if (!it.ended && it.endsAt && typeof it.endsAt === 'number') scheduleItemEnd(gw.id, it.id);
    }
  }
  processAwaitingQueue().catch(err=>console.error('processAwaitingQueue startup error', err));
});

// ---------- login ----------
if (!process.env.BOT_TOKEN) { console.error('Missing BOT_TOKEN in .env'); process.exit(1); }
client.login(process.env.BOT_TOKEN).catch(err => { console.error('Login failed', err); process.exit(1); });
