'use strict';
/**
 * InnerTube payload parsers — Vandal. Pure functions, no I/O.
 * (Source is intentionally split across modules.)
 */

const deepFind = (obj, key, limit = 1) => {
  const out = [];
  const stack = [obj];
  while (stack.length && out.length < limit) {
    const cur = stack.pop();
    if (cur && typeof cur === 'object') {
      if (Array.isArray(cur)) { for (const v of cur) stack.push(v); continue; }
      if (Object.prototype.hasOwnProperty.call(cur, key)) out.push(cur[key]);
      for (const k in cur) {
        const v = cur[k];
        if (v && typeof v === 'object') stack.push(v);
      }
    }
  }
  return out.length ? out : null;
};

const textOf = (t) => {
  if (!t) return '';
  if (typeof t === 'string') return t;
  if (t.simpleText != null) return String(t.simpleText);
  if (typeof t.content === 'string') return t.content;
  if (t.text?.content != null) return String(t.text.content);
  if (t.dynamicTextViewModel) return textOf(t.dynamicTextViewModel.text);
  if (Array.isArray(t.runs)) return t.runs.map(r => r.text || '').join('');
  return '';
};

const bestThumb = (thumbs) => {
  if (!Array.isArray(thumbs) || !thumbs.length) return '';
  return String(thumbs[thumbs.length - 1].url || '');
};

const parseRuns = (runs) => (Array.isArray(runs) ? runs.map(r => r.text || '').join('') : '');

const durText = (t) => {
  const m = /^(\d+:)?\d{1,2}:\d{2}$/.test(t || '');
  return m ? t : '';
};

/* ------------------------------------------------------- item normalization */

function endpointToUrl(cmd) {
  if (!cmd || typeof cmd !== 'object') return null;
  const meta = cmd.commandMetadata?.webCommandMetadata?.url;
  if (cmd.watchEndpoint?.videoId) return '/watch?v=' + cmd.watchEndpoint.videoId;
  if (cmd.reelWatchEndpoint?.videoId) return '/shorts/' + cmd.reelWatchEndpoint.videoId;
  if (cmd.watchPlaylistEndpoint?.playlistId) return '/playlist?list=' + cmd.watchPlaylistEndpoint.playlistId;
  if (cmd.browseEndpoint?.browseId) {
    const cu = cmd.browseEndpoint.canonicalBaseUrl || '';
    if (cu.startsWith('/@')) return '/channel/' + cu.slice(1);
    return '/channel/' + cmd.browseEndpoint.browseId;
  }
  if (cmd.playlistEditEndpoint) return null;
  return meta || null;
}

/** modern unified renderer (2024+): lockupViewModel */
function parseLockup(lvm) {
  if (!lvm || typeof lvm !== 'object') return null;
  const contentId = lvm.contentId;
  const ctype = lvm.contentType || '';
  const md = lvm.metadata?.lockupMetadataViewModel;
  const title = textOf(md?.title);
  const metaRows = md?.metadata?.contentMetadataViewModel?.metadataRows || [];
  const SEPARATORS = new Set(['•', '·', '・', '-']);
  const rows = metaRows
    .map(r => (r.metadataParts || [])
      .map(p => textOf(p.text))
      .filter(t => t && !SEPARATORS.has(t.trim())))
    .filter(r => r.length);
  let url = null;
  const cmd = lvm.rendererContext?.commandContext?.onTap?.innertubeCommand
    || lvm.rendererContext?.commandContext?.onTap?.command?.innertubeCommand;
  url = endpointToUrl(cmd);

  let kind = 'video';
  if (ctype.includes('PLAYLIST')) kind = 'playlist';
  else if (ctype.includes('CHANNEL')) kind = 'channel';
  else if (ctype.includes('SHORT') || (url && url.startsWith('/shorts/'))) kind = 'short';
  else if (url && url.startsWith('/channel/')) kind = 'channel';
  else if (url && url.startsWith('/playlist')) kind = 'playlist';

  // プレイリスト/ミックス: YouTube 本家同様、カードは watch URL に list= を
  // 伴う形へ正規化する（list を捨てると視聴ページにパネルが出ない根本原因）
  if (kind === 'playlist' && contentId) {
    if (url && url.startsWith('/watch?v=')) {
      if (!/[?&]list=/.test(url)) url += '&list=' + encodeURIComponent(contentId);
    } else {
      url = '/playlist?list=' + encodeURIComponent(contentId);
    }
  }

  // thumbnail: video lockups use thumbnailViewModel; channels use avatar models
  let thumb = '';
  const ci = lvm.contentImage || {};
  const tvm = ci.thumbnailViewModel || ci.collectionThumbnailViewModel?.primaryThumbnail?.thumbnailViewModel;
  if (tvm?.image?.sources) thumb = bestThumb(tvm.image.sources);
  if (!thumb) {
    const av = deepFind(ci, 'avatarViewModel', 1);
    if (av?.[0]?.image?.sources) thumb = bestThumb(av[0].image.sources);
  }
  if (!thumb) {
    const dec = deepFind(ci, 'decoratedAvatarViewModel', 1)?.[0];
    const srcs = dec?.avatar?.avatarViewModel?.image?.sources;
    if (srcs) thumb = bestThumb(srcs);
  }

  // duration badge
  let duration = '';
  const badges = deepFind(tvm || {}, 'thumbnailBadgeViewModel', 3) || [];
  for (const b of badges) {
    const t = textOf(b);
    const dt = durText((t || '').trim());
    if (dt) { duration = dt; break; }
  }

  const id = contentId
    || (url?.match(/[?&]v=([\w-]{11})/)?.[1])
    || (url?.match(/shorts\/([\w-]{11})/)?.[1]);
  if (!id && !url) return null;

  // チャンネル行（アバター + 名前）と再生回数行を分離する。
  // 2024+ lockupViewModel は通常 1 行目=チャンネル、2 行目=再生回数・投稿日。
  // （動画・ショートのみ。チャンネル/再生リストの行は統計なので対象外）
  let channel = '';
  let channelAvatar = '';
  let metaTop = rows[0] || [];
  let metaBottom = rows[1] || [];
  if (kind === 'video' || kind === 'short') {
    let channelRowIdx = -1;
    for (let i = 0; i < metaRows.length; i++) {
      const parts = metaRows[i]?.metadataParts || [];
      for (const part of parts) {
        const av = part?.avatarViewModel?.image?.sources
          || part?.decoratedAvatarViewModel?.avatar?.avatarViewModel?.image?.sources;
        if (av && !channelAvatar) channelAvatar = bestThumb(av);
      }
      if (/avatarViewModel|decoratedAvatarViewModel/.test(JSON.stringify(parts)) && rows[i]?.join('')) {
        channel = rows[i].join('');
        channelRowIdx = i;
        break;
      }
    }
    if (channelRowIdx < 0) {
      // アバターが無い場合: 統計（回視聴・登録者数など）らしくない先頭行をチャンネル名とみなす
      const STATS_HINT = /回視聴|回再生|視聴|views?|登録者|subscriber|動画|videos?|前$|^\d/i;
      for (let i = 0; i < rows.length; i++) {
        const t = rows[i].join('');
        if (t && !STATS_HINT.test(t)) { channel = t; channelRowIdx = i; break; }
      }
    }
    if (channelRowIdx >= 0) {
      const rest = rows.filter((_, j) => j !== channelRowIdx);
      metaTop = rest[0] || [];
      metaBottom = rest[1] || [];
    }
  }

  return {
    kind, id: id || '',
    url: url || (id ? '/watch?v=' + id : null),
    title,
    thumb,
    duration,
    channel,
    channelAvatar,
    metaTop,
    metaBottom,
  };
}

function channelAvatarOf(v) {
  return bestThumb(
    v?.channelThumbnailSupportedRenderers?.channelThumbnailWithLinkRenderer?.thumbnail?.thumbnails
    || v?.channelThumbnail?.thumbnails
  );
}

function parseVideoRenderer(v) {
  if (!v?.videoId) return null;
  const ownerRuns = v.ownerText?.runs || v.longBylineText?.runs || [];
  const ch = ownerRuns[0]?.navigationEndpoint?.browseEndpoint;
  return {
    kind: 'video',
    id: v.videoId,
    url: '/watch?v=' + v.videoId,
    title: textOf(v.title),
    thumb: bestThumb(v.thumbnail?.thumbnails) || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
    duration: textOf(v.lengthText) || '',
    badges: [],
    views: textOf(v.viewCountText),
    published: textOf(v.publishedTimeText),
    channelId: ch?.browseId || '',
    channel: textOf(v.ownerText),
    channelAvatar: channelAvatarOf(v),
    metaTop: [textOf(v.viewCountText), textOf(v.publishedTimeText)].filter(Boolean),
    metaBottom: [],
  };
}

function parseCompactVideo(v) {
  if (!v?.videoId) return null;
  const base = parseVideoRenderer(v);
  return { ...base };
}

function parseReelItem(rr) {
  if (!rr?.videoId) return null;
  const onTap = rr.navigationEndpoint;
  return {
    kind: 'short',
    id: rr.videoId,
    url: '/shorts/' + rr.videoId,
    title: textOf(rr.headline),
    thumb: bestThumb(rr.thumbnail?.thumbnails) || `https://i.ytimg.com/vi/${rr.videoId}/oar2.jpg`,
    views: textOf(rr.viewCountText),
    accessibility: textOf(rr.accessibility?.accessibilityData?.label),
  };
}

/** 2025+ shorts shelf item: shortsLockupViewModel */
function parseShortsLockup(sl) {
  if (!sl || typeof sl !== 'object') return null;
  const cmd = sl.onTap?.innertubeCommand;
  const id = cmd?.reelWatchEndpoint?.videoId
    || String(sl.entityId || '').match(/shorts-shelf-item-([\w-]{11})/)?.[1];
  if (!id) return null;
  const thumb = bestThumb(sl.thumbnailViewModel?.image?.sources)
    || bestThumb(cmd?.reelWatchEndpoint?.thumbnail?.thumbnails)
    || `https://i.ytimg.com/vi/${id}/oar2.jpg`;
  let title = textOf(sl.overlayMetadata?.primaryText)
    || textOf(sl.overlayMetadata?.primaryText?.content)
    || '';
  let views = textOf(sl.overlayMetadata?.secondaryText) || '';
  if (!title && sl.accessibilityText) {
    title = String(sl.accessibilityText).replace(/,?\s*[\d,.万億]*\s*回視聴\s*-\s*ショート動画を再生\s*$/, '').trim();
    const vm = String(sl.accessibilityText).match(/([\d,.万億]+\s*回視聴)/);
    if (!views && vm) views = vm[1];
  }
  return { kind: 'short', id, url: '/shorts/' + id, title, thumb, views };
}

function parsePlaylistVideo(v) {
  if (!v?.videoId) return null;
  return {
    kind: 'video',
    id: v.videoId,
    url: '/watch?v=' + v.videoId,
    title: textOf(v.title),
    thumb: bestThumb(v.thumbnail?.thumbnails) || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
    duration: textOf(v.lengthText) || '',
    channel: textOf(v.shortBylineText),
    index: v.index ? Number(textOf(v.index)) : undefined,
  };
}

/** watch-page/mix playlist panel item */
function parsePanelVideo(v) {
  if (!v?.videoId) return null;
  return {
    kind: 'video',
    id: v.videoId,
    url: '/watch?v=' + v.videoId,
    title: textOf(v.title),
    thumb: bestThumb(v.thumbnail?.thumbnails) || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
    duration: textOf(v.lengthText) || '',
    channel: textOf(v.shortBylineText) || textOf(v.longBylineText) || '',
    selected: !!v.selected,
  };
}

function parseChannelRenderer(c) {
  if (!c?.channelId) return null;
  const canon = c.navigationEndpoint?.browseEndpoint?.canonicalBaseUrl
    || c.navigationEndpoint?.commandMetadata?.webCommandMetadata?.url || '';
  return {
    kind: 'channel',
    id: c.channelId,
    url: '/channel/' + c.channelId,
    title: textOf(c.title),
    handle: canon.startsWith('/@') ? canon.slice(1) : '',
    subs: textOf(c.subscriberCountText),
    videos: textOf(c.videoCountText),
    thumb: bestThumb(c.thumbnail?.thumbnails),
    description: textOf(c.descriptionSnippet),
  };
}

function parsePlaylistRenderer(p) {
  if (!p?.playlistId) return null;
  return {
    kind: 'playlist',
    id: p.playlistId,
    url: '/playlist?list=' + p.playlistId,
    title: textOf(p.title),
    count: textOf(p.videoCountText) || textOf(p.videoCount),
    thumb: bestThumb(p.thumbnails || p.thumbnail?.thumbnails || p.thumbnailRenderer?.showCustomThumbnailRenderer?.thumbnail?.thumbnails),
    channel: textOf(p.shortBylineText),
  };
}

/**
 * Walk an InnerTube response tree once and pull out normalized content items
 * in visual order, plus the continuation token for the next page.
 */
function extractItems(root) {
  const items = [];
  const seen = new Set();
  let continuation = null;
  const push = (kind, it) => {
    if (!it) return;
    const key = kind + ':' + (it.id || it.url || it.title);
    if (seen.has(key)) return;
    seen.add(key);
    items.push(it);
  };
  const walk = (node) => {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const v of node) walk(v); return; }
    const keys = Object.keys(node);
    if (node.lockupViewModel) { push('lockup', parseLockup(node.lockupViewModel)); return; }
    if (node.shortsLockupViewModel) { push('short', parseShortsLockup(node.shortsLockupViewModel)); return; }
    if (node.richItemRenderer?.content) { walk(node.richItemRenderer.content); return; }
    if (node.reelItemRenderer) { push('short', parseReelItem(node.reelItemRenderer)); return; }
    if (node.gridVideoRenderer) { push('video', parseVideoRenderer(node.gridVideoRenderer)); return; }
    if (node.compactVideoRenderer) { push('video', parseCompactVideo(node.compactVideoRenderer)); return; }
    if (node.videoRenderer) { push('video', parseVideoRenderer(node.videoRenderer)); return; }
    if (node.playlistVideoRenderer) { push('video', parsePlaylistVideo(node.playlistVideoRenderer)); return; }
    if (node.playlistPanelVideoRenderer) { push('video', parsePanelVideo(node.playlistPanelVideoRenderer)); return; }
    if (node.channelRenderer) { push('channel', parseChannelRenderer(node.channelRenderer)); return; }
    if (node.playlistRenderer) { push('playlist', parsePlaylistRenderer(node.playlistRenderer)); return; }
    if (node.continuationItemRenderer) {
      const tok = node.continuationItemRenderer.continuationEndpoint?.continuationCommand?.token
        || deepFind(node.continuationItemRenderer, 'token', 1)?.[0];
      if (tok) continuation = tok;
      return;
    }
    // don't descend into ad/nudge renderers
    for (const k of keys) {
      if (k === 'adSlotRenderer' || k === 'feedNudgeRenderer' || k === 'statementBannerRenderer' || k === 'promotedSparklesWebRenderer') continue;
      walk(node[k]);
    }
  };
  walk(root);
  return { items, continuation };
}

module.exports = {
  deepFind, textOf, bestThumb, parseRuns, durText, endpointToUrl, extractItems,
  parseLockup, parseVideoRenderer, channelAvatarOf, parseCompactVideo, parseReelItem,
  parseShortsLockup, parsePlaylistVideo, parsePanelVideo, parseChannelRenderer, parsePlaylistRenderer,
};
