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
  function speak(text) {
    if (!state.sound || !('speechSynthesis' in window)) return;
    speechSynthesis.cancel();
    const u = new SpeechSynthesisUtterance(text);
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
