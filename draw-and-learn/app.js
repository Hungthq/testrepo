(() => {
  'use strict';

  // ---------- Content ----------
  const COLORS = [
    { name: 'Red', hex: '#ef476f' },
    { name: 'Orange', hex: '#ff9f1c' },
    { name: 'Yellow', hex: '#ffd60a' },
    { name: 'Green', hex: '#06d6a0' },
    { name: 'Blue', hex: '#118ab2' },
    { name: 'Purple', hex: '#8338ec' },
    { name: 'Pink', hex: '#ff70a6' },
    { name: 'Brown', hex: '#8d5524' },
    { name: 'Black', hex: '#222222' },
  ];

  const LETTERS = [
    ['A', 'Apple', '🍎'], ['B', 'Ball', '⚽'], ['C', 'Cat', '🐱'], ['D', 'Dog', '🐶'],
    ['E', 'Elephant', '🐘'], ['F', 'Fish', '🐟'], ['G', 'Grapes', '🍇'], ['H', 'House', '🏠'],
    ['I', 'Ice cream', '🍦'], ['J', 'Juice', '🧃'], ['K', 'Kite', '🪁'], ['L', 'Lion', '🦁'],
    ['M', 'Moon', '🌙'], ['N', 'Nose', '👃'], ['O', 'Orange', '🍊'], ['P', 'Pig', '🐷'],
    ['Q', 'Queen', '👸'], ['R', 'Rainbow', '🌈'], ['S', 'Sun', '☀️'], ['T', 'Tree', '🌳'],
    ['U', 'Umbrella', '☂️'], ['V', 'Violin', '🎻'], ['W', 'Whale', '🐳'], ['X', 'Fox', '🦊'],
    ['Y', 'Yo-yo', '🪀'], ['Z', 'Zebra', '🦓'],
  ].map(([ch, word, emoji]) => ({
    glyph: ch,
    emoji,
    text: ch === 'X' ? `X is in ${word}` : `${ch} is for ${word}`,
    say: ch === 'X' ? `X. X is in ${word}` : `${ch}. ${ch} is for ${word}`,
  }));

  const NUMBER_WORDS = ['Zero', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten'];
  const COUNT_EMOJI = ['🍓', '🐞', '🐥', '🍪', '🌸', '🐟', '🍌', '⭐', '🦋', '🍒'];
  const NUMBERS = NUMBER_WORDS.map((word, n) => ({
    glyph: String(n),
    emoji: n === 0 ? '🫙' : COUNT_EMOJI[(n - 1) % COUNT_EMOJI.length].repeat(n),
    text: `${n} — ${word}`,
    say: n === 0 ? 'Zero. Nothing at all!' : `${word}. Let's count: ${NUMBER_WORDS.slice(1, n + 1).join(', ')}!`,
  }));

  const SHAPES = [
    { shape: 'circle', emoji: '⚪', text: 'Circle', say: 'Circle. A circle is round, like a ball!' },
    { shape: 'square', emoji: '🟥', text: 'Square', say: 'Square. A square has four equal sides!' },
    { shape: 'triangle', emoji: '🔺', text: 'Triangle', say: 'Triangle. A triangle has three sides!' },
    { shape: 'rectangle', emoji: '🚪', text: 'Rectangle', say: 'Rectangle. A rectangle is like a door!' },
    { shape: 'star', emoji: '⭐', text: 'Star', say: 'Star. A star has five points!' },
    { shape: 'heart', emoji: '❤️', text: 'Heart', say: 'Heart. I love you!' },
    { shape: 'diamond', emoji: '🔷', text: 'Diamond', say: 'Diamond. A diamond looks like a kite!' },
    { shape: 'oval', emoji: '🥚', text: 'Oval', say: 'Oval. An oval is shaped like an egg!' },
  ];

  const LESSONS = { letters: LETTERS, numbers: NUMBERS, shapes: SHAPES };
  const STAMPS = ['⭐', '❤️', '🌸', '🦋', '🐶', '🐱', '🌈', '☀️', '🍎', '🚗', '🎈', '🦄'];
  const PRAISE = ['Great job!', 'Awesome!', 'You did it!', 'Super star!', 'Wonderful!', 'Amazing!'];
  const GUIDE_FONT = '"Arial Rounded MT Bold", "Arial Black", Arial, sans-serif';

  // ---------- State ----------
  const state = {
    mode: 'free',
    color: COLORS[4].hex,
    size: 14,
    tool: 'brush', // brush | rainbow | eraser | stamp
    stamp: STAMPS[0],
    hue: 0,
    sound: true,
    lang: 'en',
    index: { letters: 0, numbers: 0, shapes: 0 },
  };

  // ---------- Elements ----------
  const $ = (id) => document.getElementById(id);
  const board = $('board');
  const guideCanvas = $('guideCanvas');
  const drawCanvas = $('drawCanvas');
  const gctx = guideCanvas.getContext('2d');
  const dctx = drawCanvas.getContext('2d');
  const celebrateEl = $('celebrate');

  let dpr = 1;
  let boardW = 0;
  let boardH = 0;

  // ---------- Sound ----------
  function speak(text, lang = 'en') {
    if (!state.sound || !text || !('speechSynthesis' in window)) return;
    speechSynthesis.cancel();
    const u = new SpeechSynthesisUtterance(text);
    u.lang = lang === 'vi' ? 'vi-VN' : 'en-US';
    u.rate = 0.85;
    u.pitch = 1.25;
    speechSynthesis.speak(u);
  }

  let audioCtx = null;
  function chime(notes) {
    if (!state.sound) return;
    try {
      audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
      const t0 = audioCtx.currentTime;
      notes.forEach((freq, i) => {
        const osc = audioCtx.createOscillator();
        const gain = audioCtx.createGain();
        osc.type = 'triangle';
        osc.frequency.value = freq;
        const t = t0 + i * 0.12;
        gain.gain.setValueAtTime(0.0001, t);
        gain.gain.exponentialRampToValueAtTime(0.25, t + 0.02);
        gain.gain.exponentialRampToValueAtTime(0.0001, t + 0.35);
        osc.connect(gain).connect(audioCtx.destination);
        osc.start(t);
        osc.stop(t + 0.4);
      });
    } catch (_) { /* audio not available */ }
  }

  // ---------- Canvas sizing ----------
  function resize() {
    const rect = board.getBoundingClientRect();
    const newDpr = window.devicePixelRatio || 1;
    if (rect.width === boardW && rect.height === boardH && newDpr === dpr) return;

    // Keep the existing drawing across resizes.
    const keep = document.createElement('canvas');
    keep.width = drawCanvas.width;
    keep.height = drawCanvas.height;
    if (keep.width && keep.height) keep.getContext('2d').drawImage(drawCanvas, 0, 0);

    dpr = newDpr;
    boardW = rect.width;
    boardH = rect.height;
    for (const c of [guideCanvas, drawCanvas]) {
      c.width = Math.round(boardW * dpr);
      c.height = Math.round(boardH * dpr);
    }
    dctx.setTransform(1, 0, 0, 1, 0, 0);
    if (keep.width && keep.height) dctx.drawImage(keep, 0, 0);
    dctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    gctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    drawGuide();
  }

  // ---------- Guides (letters / numbers / shapes) ----------
  function currentLesson() {
    const list = LESSONS[state.mode];
    return list ? list[state.index[state.mode]] : null;
  }

  function glyphFontSize(w, h) {
    return Math.min(w * 0.8, h * 0.82);
  }

  // Builds the path for a shape centered in a w x h area.
  function shapePath(ctx, shape, w, h) {
    const cx = w / 2;
    const cy = h / 2;
    const s = Math.min(w, h) * 0.36;
    ctx.beginPath();
    switch (shape) {
      case 'circle':
        ctx.arc(cx, cy, s, 0, Math.PI * 2);
        break;
      case 'square':
        ctx.rect(cx - s, cy - s, s * 2, s * 2);
        break;
      case 'rectangle': {
        const rw = Math.min(w * 0.4, s * 1.7);
        ctx.rect(cx - rw, cy - s * 0.8, rw * 2, s * 1.6);
        break;
      }
      case 'triangle':
        ctx.moveTo(cx, cy - s * 1.1);
        ctx.lineTo(cx + s * 1.2, cy + s * 0.9);
        ctx.lineTo(cx - s * 1.2, cy + s * 0.9);
        ctx.closePath();
        break;
      case 'diamond':
        ctx.moveTo(cx, cy - s * 1.15);
        ctx.lineTo(cx + s * 0.8, cy);
        ctx.lineTo(cx, cy + s * 1.15);
        ctx.lineTo(cx - s * 0.8, cy);
        ctx.closePath();
        break;
      case 'oval':
        ctx.ellipse(cx, cy, Math.min(w * 0.42, s * 1.5), s * 0.9, 0, 0, Math.PI * 2);
        break;
      case 'star':
        for (let i = 0; i < 10; i++) {
          const r = i % 2 === 0 ? s * 1.15 : s * 0.48;
          const a = -Math.PI / 2 + (i * Math.PI) / 5;
          const x = cx + Math.cos(a) * r;
          const y = cy + Math.sin(a) * r + s * 0.1;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        ctx.closePath();
        break;
      case 'heart': {
        const top = cy - s * 0.55;
        ctx.moveTo(cx, cy + s * 1.05);
        ctx.bezierCurveTo(cx - s * 1.6, cy + s * 0.05, cx - s * 1.05, top - s * 0.95, cx, top);
        ctx.bezierCurveTo(cx + s * 1.05, top - s * 0.95, cx + s * 1.6, cy + s * 0.05, cx, cy + s * 1.05);
        ctx.closePath();
        break;
      }
    }
  }

  function shapeLineWidth(w, h) {
    return Math.max(18, Math.min(w, h) * 0.07);
  }

  // Paints the lesson's guide onto ctx. `solid` is used for the hidden scoring mask.
  function paintGuide(ctx, lesson, w, h, solid, extra = 0) {
    if (lesson.glyph) {
      const fs = glyphFontSize(w, h);
      ctx.font = `bold ${fs}px ${GUIDE_FONT}`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      const y = h / 2 + fs * 0.04;
      if (solid) {
        ctx.fillStyle = ctx.strokeStyle = '#000';
        ctx.fillText(lesson.glyph, w / 2, y);
        if (extra) {
          ctx.lineWidth = extra;
          ctx.lineJoin = 'round';
          ctx.strokeText(lesson.glyph, w / 2, y);
        }
      } else {
        ctx.fillStyle = '#eceaf6';
        ctx.fillText(lesson.glyph, w / 2, y);
        ctx.setLineDash([fs * 0.03, fs * 0.03]);
        ctx.lineWidth = Math.max(2, fs * 0.008);
        ctx.strokeStyle = '#a9a3c9';
        ctx.strokeText(lesson.glyph, w / 2, y);
        ctx.setLineDash([]);
      }
    } else if (lesson.shape) {
      const lw = shapeLineWidth(w, h);
      shapePath(ctx, lesson.shape, w, h);
      ctx.lineJoin = 'round';
      ctx.lineCap = 'round';
      if (solid) {
        ctx.strokeStyle = '#000';
        ctx.lineWidth = lw + extra;
        ctx.stroke();
      } else {
        ctx.strokeStyle = '#eceaf6';
        ctx.lineWidth = lw;
        ctx.stroke();
        ctx.setLineDash([lw * 0.5, lw * 0.6]);
        ctx.strokeStyle = '#a9a3c9';
        ctx.lineWidth = Math.max(2, lw * 0.12);
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }
  }

  function drawGuide() {
    gctx.clearRect(0, 0, boardW, boardH);
    const lesson = currentLesson();
    if (lesson) paintGuide(gctx, lesson, boardW, boardH, false);
  }

  // ---------- Drawing ----------
  const undoStack = [];
  const UNDO_LIMIT = 25;

  function pushUndo() {
    try {
      undoStack.push(dctx.getImageData(0, 0, drawCanvas.width, drawCanvas.height));
      if (undoStack.length > UNDO_LIMIT) undoStack.shift();
    } catch (_) { /* ignore */ }
  }

  function undo() {
    const img = undoStack.pop();
    if (!img) return;
    if (img.width !== drawCanvas.width || img.height !== drawCanvas.height) {
      undoStack.length = 0;
      return;
    }
    dctx.putImageData(img, 0, 0);
    hideCelebrate();
  }

  function clearDrawing(record = true) {
    if (record) pushUndo();
    dctx.save();
    dctx.setTransform(1, 0, 0, 1, 0, 0);
    dctx.clearRect(0, 0, drawCanvas.width, drawCanvas.height);
    dctx.restore();
    hideCelebrate();
  }

  function strokeStyleNow() {
    if (state.tool === 'rainbow') {
      state.hue = (state.hue + 4) % 360;
      return `hsl(${state.hue}, 90%, 55%)`;
    }
    return state.color;
  }

  function segment(from, to) {
    dctx.globalCompositeOperation = state.tool === 'eraser' ? 'destination-out' : 'source-over';
    dctx.strokeStyle = strokeStyleNow();
    dctx.lineWidth = state.tool === 'eraser' ? state.size * 2 : state.size;
    dctx.lineCap = 'round';
    dctx.lineJoin = 'round';
    dctx.beginPath();
    dctx.moveTo(from.x, from.y);
    dctx.lineTo(to.x, to.y);
    dctx.stroke();
    dctx.globalCompositeOperation = 'source-over';
  }

  function placeStamp(p) {
    const size = state.size * 4 + 24;
    dctx.font = `${size}px "Apple Color Emoji", "Segoe UI Emoji", "Noto Color Emoji", sans-serif`;
    dctx.textAlign = 'center';
    dctx.textBaseline = 'middle';
    dctx.fillText(state.stamp, p.x, p.y);
    chime([880]);
  }

  let activePointer = null;
  let last = null;

  function pointFrom(e) {
    const rect = drawCanvas.getBoundingClientRect();
    return { x: e.clientX - rect.left, y: e.clientY - rect.top };
  }

  drawCanvas.addEventListener('pointerdown', (e) => {
    if (activePointer !== null) return; // ignore extra fingers
    e.preventDefault();
    closeStampPicker();
    hideCelebrate();
    activePointer = e.pointerId;
    drawCanvas.setPointerCapture(e.pointerId);
    pushUndo();
    const p = pointFrom(e);
    if (state.tool === 'stamp') {
      placeStamp(p);
      last = null;
      return;
    }
    last = p;
    segment(p, { x: p.x + 0.01, y: p.y + 0.01 }); // dot on tap
  });

  drawCanvas.addEventListener('pointermove', (e) => {
    if (e.pointerId !== activePointer || !last) return;
    e.preventDefault();
    const events = e.getCoalescedEvents ? e.getCoalescedEvents() : [e];
    for (const ev of (events.length ? events : [e])) {
      const p = pointFrom(ev);
      segment(last, p);
      last = p;
    }
  });

  function endStroke(e) {
    if (e.pointerId !== activePointer) return;
    activePointer = null;
    last = null;
  }
  drawCanvas.addEventListener('pointerup', endStroke);
  drawCanvas.addEventListener('pointercancel', endStroke);

  // ---------- Tracing check ----------
  // Compares the child's drawing to the guide on a small grid.
  function scoreTracing(lesson) {
    const scale = 220 / Math.max(boardW, boardH);
    const w = Math.max(1, Math.round(boardW * scale));
    const h = Math.max(1, Math.round(boardH * scale));

    const mk = () => {
      const c = document.createElement('canvas');
      c.width = w; c.height = h;
      const ctx = c.getContext('2d', { willReadFrequently: true });
      ctx.scale(scale, scale);
      return ctx;
    };

    // Target area the child should cover.
    const target = mk();
    paintGuide(target, lesson, boardW, boardH, true);

    // Generous area where drawing still counts as "on the guide".
    const tolerance = lesson.glyph ? glyphFontSize(boardW, boardH) * 0.12 : shapeLineWidth(boardW, boardH) * 1.5;
    const zone = mk();
    paintGuide(zone, lesson, boardW, boardH, true, tolerance);

    // Drawing, thickened so a line through the middle of a thick stroke covers it.
    const thick = lesson.glyph ? glyphFontSize(boardW, boardH) * 0.07 : shapeLineWidth(boardW, boardH) * 0.6;
    const fat = mk();
    fat.setTransform(1, 0, 0, 1, 0, 0);
    const r = thick * scale;
    for (let a = 0; a < Math.PI * 2; a += Math.PI / 8) {
      fat.drawImage(drawCanvas, Math.cos(a) * r, Math.sin(a) * r, w, h);
    }
    fat.drawImage(drawCanvas, 0, 0, w, h);

    const raw = mk();
    raw.setTransform(1, 0, 0, 1, 0, 0);
    raw.drawImage(drawCanvas, 0, 0, w, h);

    const T = target.getImageData(0, 0, w, h).data;
    const Z = zone.getImageData(0, 0, w, h).data;
    const F = fat.getImageData(0, 0, w, h).data;
    const R = raw.getImageData(0, 0, w, h).data;

    let need = 0, covered = 0, drawn = 0, inside = 0;
    for (let i = 3; i < T.length; i += 4) {
      if (T[i] > 128) {
        need++;
        if (F[i] > 40) covered++;
      }
      if (R[i] > 40) {
        drawn++;
        if (Z[i] > 0) inside++;
      }
    }
    return {
      coverage: need ? covered / need : 0,
      accuracy: drawn ? inside / drawn : 0,
      drawn,
    };
  }

  function checkTracing() {
    const lesson = currentLesson();
    if (!lesson) return;
    const { coverage, accuracy, drawn } = scoreTracing(lesson);

    if (drawn === 0) {
      showMessage('✏️', 'Trace the gray lines!');
      speak('Use your finger to trace the gray lines!');
      return;
    }
    if (accuracy < 0.55) {
      showMessage('🙂', 'Try to stay on the lines!');
      speak('Good try! Try to stay on the lines.');
      return;
    }
    if (coverage < 0.6) {
      showMessage('💪', 'Keep going!');
      speak('Keep going! You are almost there.');
      return;
    }
    const stars = coverage > 0.85 && accuracy > 0.75 ? 3 : coverage > 0.72 ? 2 : 1;
    const praise = PRAISE[Math.floor(Math.random() * PRAISE.length)];
    showMessage('⭐'.repeat(stars), praise);
    chime([523, 659, 784, 1047]);
    speak(praise);
  }

  let celebrateTimer = null;
  function showMessage(top, text) {
    $('stars').textContent = top;
    $('praise').textContent = text;
    celebrateEl.classList.remove('hidden');
    // restart animation
    celebrateEl.style.animation = 'none';
    void celebrateEl.offsetWidth;
    celebrateEl.style.animation = '';
    clearTimeout(celebrateTimer);
    celebrateTimer = setTimeout(hideCelebrate, 2600);
  }
  function hideCelebrate() {
    celebrateEl.classList.add('hidden');
  }

  // ---------- Modes & lessons ----------
  function setMode(mode) {
    state.mode = mode;
    document.querySelectorAll('.mode-btn').forEach((b) => b.classList.toggle('active', b.dataset.mode === mode));
    $('lessonBar').classList.toggle('hidden', mode === 'free');
    $('magicBtn').classList.toggle('hidden', mode !== 'free');
    clearDrawing(false);
    undoStack.length = 0;
    if (mode !== 'free' && state.tool === 'stamp') setTool('brush');
    showLesson(true);
  }

  function showLesson(announce) {
    drawGuide();
    const lesson = currentLesson();
    if (!lesson) return;
    $('lessonEmoji').textContent = lesson.emoji;
    $('lessonText').textContent = lesson.text;
    if (announce) speak(lesson.say);
  }

  function stepLesson(delta) {
    const list = LESSONS[state.mode];
    if (!list) return;
    state.index[state.mode] = (state.index[state.mode] + delta + list.length) % list.length;
    clearDrawing(false);
    undoStack.length = 0;
    showLesson(true);
  }

  // ---------- Toolbar ----------
  function buildPalette() {
    const pal = $('palette');
    COLORS.forEach((c) => {
      const b = document.createElement('button');
      b.className = 'swatch';
      b.style.background = c.hex;
      b.title = c.name;
      b.setAttribute('aria-label', c.name);
      b.addEventListener('click', () => {
        state.color = c.hex;
        if (state.tool !== 'brush') setTool('brush');
        updatePalette();
        speak(c.name);
      });
      pal.appendChild(b);
    });
    updatePalette();
  }

  function updatePalette() {
    document.querySelectorAll('.swatch').forEach((b, i) => {
      b.classList.toggle('active', state.tool === 'brush' && COLORS[i].hex === state.color);
    });
  }

  function setTool(tool) {
    state.tool = state.tool === tool && tool !== 'brush' ? 'brush' : tool;
    $('eraserBtn').classList.toggle('active', state.tool === 'eraser');
    $('rainbowBtn').classList.toggle('active', state.tool === 'rainbow');
    $('stampBtn').classList.toggle('active', state.tool === 'stamp');
    $('stampBtn').textContent = state.tool === 'stamp' ? state.stamp : '⭐';
    updatePalette();
  }

  function buildStampPicker() {
    const picker = $('stampPicker');
    STAMPS.forEach((s) => {
      const b = document.createElement('button');
      b.textContent = s;
      b.addEventListener('click', () => {
        state.stamp = s;
        state.tool = 'stamp';
        setTool('stamp');
        picker.querySelectorAll('button').forEach((x) => x.classList.toggle('active', x === b));
        closeStampPicker();
      });
      picker.appendChild(b);
    });
  }

  function closeStampPicker() {
    $('stampPicker').classList.add('hidden');
  }

  function savePicture() {
    const out = document.createElement('canvas');
    out.width = drawCanvas.width;
    out.height = drawCanvas.height;
    const o = out.getContext('2d');
    o.fillStyle = '#fff';
    o.fillRect(0, 0, out.width, out.height);
    o.drawImage(guideCanvas, 0, 0);
    o.drawImage(drawCanvas, 0, 0);
    const a = document.createElement('a');
    a.download = `my-drawing-${new Date().toISOString().slice(0, 10)}.png`;
    a.href = out.toDataURL('image/png');
    document.body.appendChild(a);
    a.click();
    a.remove();
    speak('Saved your picture!');
  }

  // ---------- "What did I draw?" (Claude looks at the drawing) ----------
  const UI = {
    en: {
      magic: 'What did I draw?', loading: ['Hmm… let me look!', 'Ooh, interesting…', 'Looking closely…'],
      empty: 'Draw something first, then press me!', notSure: 'Hmm, is it…', notQuite: 'Not quite? Is it…',
      hintPlaceholder: 'I drew a…', tryAgain: 'Try again', newDrawing: 'New drawing',
      facts: 'Facts', story: 'Story', joke: 'Joke', sound: 'Sound', sayAgain: '🔊 Again',
      gallery: 'My Gallery', galleryEmpty: 'Your drawings will show up here after you press ✨',
      errors: {
        offline: 'The magic needs a grown-up to start the Draw & Learn server. (See the README!)',
        setup: 'The magic key is missing. Ask a grown-up to set it up.',
        busy: 'So many drawings! Let\'s wait a moment and try again.',
        failed: 'Oops! My magic glasses got foggy. Let\'s try again!',
      },
    },
    vi: {
      magic: 'Bé vẽ gì đây?', loading: ['Ừm… để tớ xem nào!', 'Ồ, thú vị quá…', 'Đang nhìn kỹ…'],
      empty: 'Bé hãy vẽ gì đó trước, rồi bấm tớ nhé!', notSure: 'Ừm, có phải là…', notQuite: 'Chưa đúng à? Có phải là…',
      hintPlaceholder: 'Bé vẽ con…', tryAgain: 'Thử lại', newDrawing: 'Vẽ tranh mới',
      facts: 'Điều hay', story: 'Truyện', joke: 'Câu đố vui', sound: 'Âm thanh', sayAgain: '🔊 Nghe lại',
      gallery: 'Tranh của bé', galleryEmpty: 'Tranh của bé sẽ hiện ở đây sau khi bấm ✨',
      errors: {
        offline: 'Phép thuật cần người lớn bật máy chủ Draw & Learn. (Xem README nhé!)',
        setup: 'Thiếu chìa khoá phép thuật. Nhờ người lớn cài đặt giúp nhé.',
        busy: 'Nhiều tranh quá! Đợi một chút rồi thử lại nhé.',
        failed: 'Ối! Kính thần kỳ của tớ bị mờ rồi. Thử lại nhé!',
      },
    },
  };
  const t = (key) => UI[state.lang][key];

  const look = { result: null, image: null, thumb: null, tab: 'facts' };
  const lookModal = $('lookModal');

  function applyLanguage() {
    $('langBtn').textContent = state.lang === 'vi' ? 'VI' : 'EN';
    $('magicLabel').textContent = t('magic');
    $('hintInput').placeholder = t('hintPlaceholder');
    document.querySelectorAll('[data-t]').forEach((el) => { el.textContent = t(el.dataset.t); });
  }

  // Crops the drawing to what was actually drawn, on a white background, as PNG base64.
  function exportDrawing(maxSize) {
    const w = drawCanvas.width;
    const h = drawCanvas.height;
    const data = dctx.getImageData(0, 0, w, h).data;
    let minX = w, minY = h, maxX = -1, maxY = -1;
    for (let y = 0; y < h; y += 2) {
      for (let x = 0; x < w; x += 2) {
        if (data[(y * w + x) * 4 + 3] > 20) {
          if (x < minX) minX = x;
          if (x > maxX) maxX = x;
          if (y < minY) minY = y;
          if (y > maxY) maxY = y;
        }
      }
    }
    if (maxX < 0) return null;
    const pad = Math.max(maxX - minX, maxY - minY) * 0.1 + 10 * dpr;
    minX = Math.max(0, minX - pad); minY = Math.max(0, minY - pad);
    maxX = Math.min(w, maxX + pad); maxY = Math.min(h, maxY + pad);
    const cw = maxX - minX;
    const ch = maxY - minY;
    const scale = Math.min(1, maxSize / Math.max(cw, ch));
    const out = document.createElement('canvas');
    out.width = Math.max(1, Math.round(cw * scale));
    out.height = Math.max(1, Math.round(ch * scale));
    const o = out.getContext('2d');
    o.fillStyle = '#fff';
    o.fillRect(0, 0, out.width, out.height);
    o.drawImage(drawCanvas, minX, minY, cw, ch, 0, 0, out.width, out.height);
    return out;
  }

  function showLookState(which) {
    $('lookLoading').classList.toggle('hidden', which !== 'loading');
    $('lookError').classList.toggle('hidden', which !== 'error');
    $('lookResult').classList.toggle('hidden', which !== 'result');
  }

  async function whatDidIDraw(hint = '') {
    if (!hint) {
      const canvas = exportDrawing(768);
      if (!canvas) {
        showMessage('✏️', t('empty'));
        speak(t('empty'), state.lang);
        return;
      }
      look.image = canvas.toDataURL('image/png');
      look.thumb = (exportDrawing(240) || canvas).toDataURL('image/jpeg', 0.7);
    }
    closeStampPicker();
    clearTimeout(revealTimer);
    lookModal.classList.remove('hidden');
    showLookState('loading');
    const lines = t('loading');
    const line = lines[Math.floor(Math.random() * lines.length)];
    $('loadingText').textContent = line;
    speak(line, state.lang);

    try {
      const res = await fetch('api/look', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image: look.image, hint, lang: state.lang }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.error || (res.status === 404 || res.status === 405 ? 'offline' : 'failed'));
      showResult(body, true);
      saveToGallery(body);
    } catch (err) {
      const key = err instanceof TypeError ? 'offline' : err.message;
      const msg = t('errors')[key] || t('errors').failed;
      $('errorText').textContent = msg;
      showLookState('error');
      speak(msg, state.lang);
    }
  }

  function showResult(r, celebrate) {
    look.result = r;
    $('resultEmoji').textContent = r.emoji || '🎨';
    $('lookTitle').textContent = r.title;
    $('resultCompliment').textContent = r.compliment;
    $('guessLabel').textContent = r.recognized ? t('notQuite') : t('notSure');
    const chips = $('guessChips');
    chips.textContent = '';
    (r.guesses || []).slice(0, 3).forEach((g) => {
      const b = document.createElement('button');
      b.className = 'guess-chip';
      b.textContent = g;
      b.addEventListener('click', () => whatDidIDraw(g));
      chips.appendChild(b);
    });
    $('hintInput').value = '';
    $('challengeText').textContent = r.challenge;
    showLookState('result');
    setTab('facts', false);
    if (!celebrate) {
      speak(`${r.title} ${r.compliment}`, state.lang);
      return;
    }
    // Celebrate first: the drawing jumps to life under a shower of emoji, then the card pops up.
    lookModal.classList.add('hidden');
    chime([523, 659, 784, 1047]);
    speak(r.sound, state.lang);
    emojiRain(r.recognized && r.emoji ? r.emoji : '⭐');
    drawCanvas.classList.remove('alive');
    void drawCanvas.offsetWidth;
    drawCanvas.classList.add('alive');
    clearTimeout(revealTimer);
    revealTimer = setTimeout(() => {
      if (look.result !== r) return;
      lookModal.classList.remove('hidden');
      speak(`${r.title} ${r.compliment}`, state.lang);
    }, 1800);
  }
  let revealTimer = null;

  function setTab(tab, read = true) {
    const r = look.result;
    if (!r) return;
    look.tab = tab;
    document.querySelectorAll('.fun-tab').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
    const panel = $('funPanel');
    panel.textContent = '';
    let say = '';
    if (tab === 'facts') {
      const ul = document.createElement('ul');
      (r.facts || []).forEach((f) => {
        const li = document.createElement('li');
        li.textContent = f;
        ul.appendChild(li);
      });
      panel.appendChild(ul);
      say = (r.facts || []).join(' ');
    } else if (tab === 'sound') {
      const d = document.createElement('div');
      d.className = 'sound-word';
      d.textContent = `${r.emoji || ''} ${r.sound}`;
      panel.appendChild(d);
      say = r.sound;
    } else {
      panel.textContent = tab === 'story' ? r.story : r.joke;
      say = panel.textContent;
    }
    const again = document.createElement('button');
    again.className = 'pill-btn say-again';
    again.textContent = t('sayAgain');
    again.addEventListener('click', () => speak(say, state.lang));
    panel.appendChild(again);
    if (read) speak(say, state.lang);
  }

  function emojiRain(emoji) {
    const rain = $('emojiRain');
    const pick = emoji.trim() || '⭐';
    rain.textContent = '';
    for (let i = 0; i < 28; i++) {
      const s = document.createElement('span');
      s.textContent = pick;
      s.style.left = `${Math.random() * 100}%`;
      s.style.fontSize = `${24 + Math.random() * 36}px`;
      s.style.animationDuration = `${2 + Math.random() * 2.5}s`;
      s.style.animationDelay = `${Math.random() * 1.2}s`;
      s.style.setProperty('--fall', `${window.innerHeight + 120}px`);
      s.style.setProperty('--spin', `${(Math.random() - 0.5) * 720}deg`);
      rain.appendChild(s);
      setTimeout(() => s.remove(), 6000);
    }
  }

  function closeModal(modal) {
    clearTimeout(revealTimer);
    modal.classList.add('hidden');
    if ('speechSynthesis' in window) speechSynthesis.cancel();
    stopListening();
  }

  // Speech-to-text so kids who can't type yet can say what they drew.
  const Recognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  let recognizer = null;
  function stopListening() {
    if (recognizer) { try { recognizer.abort(); } catch (_) { /* ignore */ } }
    recognizer = null;
    $('micBtn').classList.remove('listening');
  }
  function listen() {
    if (!Recognition) return;
    if (recognizer) return stopListening();
    if ('speechSynthesis' in window) speechSynthesis.cancel();
    recognizer = new Recognition();
    recognizer.lang = state.lang === 'vi' ? 'vi-VN' : 'en-US';
    recognizer.interimResults = false;
    recognizer.maxAlternatives = 1;
    recognizer.onresult = (e) => {
      const said = e.results[0][0].transcript.trim();
      $('hintInput').value = said;
      if (said) whatDidIDraw(said);
    };
    recognizer.onend = stopListening;
    recognizer.onerror = stopListening;
    $('micBtn').classList.add('listening');
    recognizer.start();
  }

  // ---------- Gallery (kept on this device only) ----------
  const GALLERY_KEY = 'draw-and-learn-gallery';
  const GALLERY_MAX = 24;

  function loadGallery() {
    try { return JSON.parse(localStorage.getItem(GALLERY_KEY)) || []; } catch (_) { return []; }
  }

  function saveToGallery(result) {
    const items = loadGallery().filter((it) => it.thumb !== look.thumb); // a re-guess replaces the old entry
    items.unshift({ thumb: look.thumb, result, date: Date.now() });
    while (items.length) {
      try {
        localStorage.setItem(GALLERY_KEY, JSON.stringify(items.slice(0, GALLERY_MAX)));
        return;
      } catch (_) {
        items.pop(); // storage full: drop the oldest and retry
      }
    }
  }

  function openGallery() {
    const grid = $('galleryGrid');
    grid.textContent = '';
    const items = loadGallery();
    $('galleryEmpty').classList.toggle('hidden', items.length > 0);
    items.forEach((it) => {
      const b = document.createElement('button');
      b.className = 'gallery-item';
      const img = document.createElement('img');
      img.src = it.thumb;
      img.alt = it.result.thing;
      const label = document.createElement('div');
      label.textContent = `${it.result.emoji || ''} ${it.result.thing}`;
      b.append(img, label);
      b.addEventListener('click', () => {
        closeModal($('galleryModal'));
        look.thumb = it.thumb;
        look.image = null; // re-guessing needs the original drawing, so hide those controls
        lookModal.classList.remove('hidden');
        showResult(it.result, false);
        $('guessChips').parentElement.classList.add('hidden');
      });
      grid.appendChild(b);
    });
    $('galleryModal').classList.remove('hidden');
  }

  $('magicBtn').addEventListener('click', () => {
    $('guessChips').parentElement.classList.remove('hidden');
    whatDidIDraw();
  });
  $('retryBtn').addEventListener('click', () => whatDidIDraw($('hintInput').value.trim()));
  document.querySelectorAll('.fun-tab').forEach((b) => b.addEventListener('click', () => setTab(b.dataset.tab)));
  $('hintForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const hint = $('hintInput').value.trim();
    if (hint && look.image) whatDidIDraw(hint);
  });
  if (Recognition) $('micBtn').classList.remove('hidden');
  $('micBtn').addEventListener('click', listen);
  $('challengeBtn').addEventListener('click', () => {
    closeModal(lookModal);
    if (look.result) speak(look.result.challenge, state.lang);
  });
  $('newDrawingBtn').addEventListener('click', () => {
    closeModal(lookModal);
    clearDrawing(true);
  });
  $('galleryBtn').addEventListener('click', openGallery);
  $('langBtn').addEventListener('click', () => {
    state.lang = state.lang === 'en' ? 'vi' : 'en';
    try { localStorage.setItem('draw-and-learn-lang', state.lang); } catch (_) { /* ignore */ }
    applyLanguage();
  });
  document.querySelectorAll('.modal').forEach((m) => {
    m.addEventListener('click', (e) => {
      if (e.target === m || e.target.closest('[data-close]')) closeModal(m);
    });
  });
  try { state.lang = localStorage.getItem('draw-and-learn-lang') === 'vi' ? 'vi' : 'en'; } catch (_) { /* ignore */ }
  applyLanguage();

  // ---------- Wire up ----------
  document.querySelectorAll('.mode-btn').forEach((b) => b.addEventListener('click', () => setMode(b.dataset.mode)));
  document.querySelectorAll('.size-btn').forEach((b) => b.addEventListener('click', () => {
    state.size = Number(b.dataset.size);
    document.querySelectorAll('.size-btn').forEach((x) => x.classList.toggle('active', x === b));
  }));

  $('prevBtn').addEventListener('click', () => stepLesson(-1));
  $('nextBtn').addEventListener('click', () => stepLesson(1));
  $('hearBtn').addEventListener('click', () => speak(currentLesson()?.say || ''));
  $('checkBtn').addEventListener('click', checkTracing);

  $('eraserBtn').addEventListener('click', () => { closeStampPicker(); setTool('eraser'); });
  $('rainbowBtn').addEventListener('click', () => { closeStampPicker(); setTool('rainbow'); });
  $('stampBtn').addEventListener('click', () => $('stampPicker').classList.toggle('hidden'));
  $('undoBtn').addEventListener('click', undo);
  $('clearBtn').addEventListener('click', () => clearDrawing(true));
  $('saveBtn').addEventListener('click', savePicture);
  $('soundBtn').addEventListener('click', () => {
    state.sound = !state.sound;
    $('soundBtn').textContent = state.sound ? '🔊' : '🔇';
    if (!state.sound && 'speechSynthesis' in window) speechSynthesis.cancel();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') document.querySelectorAll('.modal:not(.hidden)').forEach(closeModal);
    if (e.target.tagName === 'INPUT') return;
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') { e.preventDefault(); undo(); }
    if (state.mode !== 'free') {
      if (e.key === 'ArrowRight') stepLesson(1);
      if (e.key === 'ArrowLeft') stepLesson(-1);
    }
  });

  new ResizeObserver(resize).observe(board);
  window.addEventListener('resize', resize);

  buildPalette();
  buildStampPicker();
  resize();
})();
