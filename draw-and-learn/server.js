// Draw & Learn server: serves the app and asks Claude what a child drew.
// The API key stays here on the server and is never sent to the browser.
//
//   ANTHROPIC_API_KEY=sk-ant-... npm start
//   then open http://localhost:3000 (or http://<this-computer's-IP>:3000 on a tablet)

import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Anthropic from '@anthropic-ai/sdk';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const MODEL = process.env.CLAUDE_MODEL || 'claude-opus-5-5';
const MAX_BODY = 6 * 1024 * 1024;

const client = new Anthropic();

const STATIC = {
  '/': 'index.html',
  '/index.html': 'index.html',
  '/style.css': 'style.css',
  '/app.js': 'app.js',
};
const TYPES = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript' };

const SYSTEM = `You are "Doodle Buddy", a warm, playful friend inside a drawing app for young children (about 3 to 8 years old).
A child has just drawn a picture with their finger and wants to know what you see.

How to look at the drawing:
- Children's drawings are simple and wobbly. Be generous: guess the most likely thing a child meant (a circle with lines may be the sun, four legs and a tail may be a dog or cat).
- The child may draw several things. Pick the main subject for "thing"; you can mention the rest in "compliment" or "story".
- If the child gave a hint about what they drew, trust the hint.
- If you truly cannot tell, set "recognized" to false, still say something kind and specific about the colors and shapes, and offer your best 3 guesses.

How to write:
- Short, simple words a 5-year-old understands. Short sentences. Cheerful and encouraging, never critical.
- The compliment must mention something really in the picture (a color, a shape, a detail), so the child knows you truly looked.
- Facts must be true, surprising and easy to picture. One sentence each.
- The joke must be a gentle, silly, kid-friendly joke about the thing.
- The story stars the thing the child drew as the hero. Give it a fun title and 5 to 7 sentences: a little adventure, one silly surprise, and a happy ending. Use details from the drawing (its colors, its shape) in the story.
- The song is exactly 4 short lines about the thing, set to one well-known nursery tune. Each line must fit its line of the tune, using the same number of syllables as the original words, so it can be sung:
  - "twinkle" (Twinkle Twinkle Little Star): 7, 7, 7, 7 syllables. Line 4 uses the tune of line 2.
  - "row" (Row Row Row Your Boat): 5, 5, 12, 5 syllables. Line 3 can repeat a happy word, like "Merrily, merrily…".
  - "mary" (Mary Had a Little Lamb): 7, 6, 7, 6 syllables. Line 2 can repeat a phrase, like "Little lamb, little lamb".
  - "london" (London Bridge Is Falling Down): 7, 6, 7, 4 syllables.
  Pick the tune whose rhythm suits the words best. Rhyme lines 2 and 4 if you can.
- Nothing scary, violent, sad or grown-up. If the drawing looks like something scary, turn it into something friendly and silly.
- Never ask for the child's name, address, school or any personal information.
- Keep "emoji" as one or two emoji characters.`;

const RESULT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: [
    'recognized', 'thing', 'emoji', 'guesses', 'title', 'compliment', 'facts', 'joke',
    'story_title', 'story', 'song_title', 'song_tune', 'song_lines', 'sound', 'challenge',
  ],
  properties: {
    recognized: { type: 'boolean', description: 'True if you are fairly sure what the drawing shows.' },
    thing: { type: 'string', description: 'The main thing in the drawing, 1-3 words, e.g. "cat" or "rainbow".' },
    emoji: { type: 'string', description: 'One or two emoji that best show the thing.' },
    guesses: { type: 'array', items: { type: 'string' }, description: 'Up to 3 other things it might be (short names).' },
    title: { type: 'string', description: 'An excited headline, e.g. "Wow, a cat!"' },
    compliment: { type: 'string', description: 'One or two sentences praising specific details of this drawing.' },
    facts: { type: 'array', items: { type: 'string' }, description: 'Exactly 3 fun facts for kids.' },
    joke: { type: 'string', description: 'A short, silly joke about the thing.' },
    story_title: { type: 'string', description: 'A fun title for the story, e.g. "Bubbles the Blue Cat Saves the Day".' },
    story: { type: 'string', description: 'A 5-7 sentence fun story starring the thing, with a silly surprise and a happy ending.' },
    song_title: { type: 'string', description: 'A short title for the song.' },
    song_tune: { type: 'string', enum: ['twinkle', 'row', 'mary', 'london'], description: 'Which nursery tune the song is sung to.' },
    song_lines: { type: 'array', items: { type: 'string' }, description: 'Exactly 4 song lines that fit the chosen tune.' },
    sound: { type: 'string', description: 'A fun sound the thing makes or might make, e.g. "Meow meow!" or "Whoosh!"' },
    challenge: { type: 'string', description: 'A fun idea for what to draw next to go with it.' },
  },
};

async function askClaude({ image, hint }) {
  const prompt = hint
    ? `Here is my drawing! What did I draw? Hint from the child: "${hint}".`
    : 'Here is my drawing! What did I draw?';

  const response = await client.beta.messages.create({
    model: MODEL,
    max_tokens: 6000,
    betas: ['server-side-fallback-2026-07-01'],
    fallbacks: 'default',
    // A simple, playful task: low effort keeps the answer fast for an impatient little artist.
    output_config: {
      effort: 'low',
      format: { type: 'json_schema', schema: RESULT_SCHEMA },
    },
    system: SYSTEM,
    messages: [{
      role: 'user',
      content: [
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: image } },
        { type: 'text', text: prompt },
      ],
    }],
  });

  if (response.stop_reason === 'refusal') {
    throw Object.assign(new Error('refused'), { status: 422 });
  }
  const text = response.content.find((b) => b.type === 'text')?.text;
  if (!text) throw new Error('No answer from Claude');
  return JSON.parse(text);
}

function send(res, status, body, type = 'application/json') {
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

async function readJson(req) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY) throw Object.assign(new Error('Picture is too big'), { status: 413 });
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function handleLook(req, res) {
  let body;
  try {
    body = await readJson(req);
  } catch (err) {
    return send(res, err.status || 400, { error: err.message });
  }
  const image = String(body.image || '').replace(/^data:image\/png;base64,/, '');
  if (!image || !/^[A-Za-z0-9+/=]+$/.test(image)) {
    return send(res, 400, { error: 'Missing drawing' });
  }
  const hint = String(body.hint || '').slice(0, 80).replace(/["\n\r]/g, ' ').trim();

  try {
    const result = await askClaude({ image, hint });
    send(res, 200, result);
  } catch (err) {
    if (err instanceof Anthropic.AuthenticationError) {
      console.error('Claude API key is missing or invalid. Set ANTHROPIC_API_KEY.');
      return send(res, 500, { error: 'setup' });
    }
    if (err instanceof Anthropic.RateLimitError) {
      return send(res, 429, { error: 'busy' });
    }
    if (err instanceof Anthropic.APIConnectionError) {
      console.error('Could not reach the Claude API:', err.message);
      return send(res, 503, { error: 'offline' });
    }
    console.error('Look failed:', err.status || '', err.message);
    send(res, err.status === 422 ? 422 : 500, { error: 'failed' });
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'POST' && url.pathname === '/api/look') return handleLook(req, res);
  if (req.method === 'GET' && url.pathname === '/api/health') return send(res, 200, { ok: true });

  const file = STATIC[url.pathname];
  if (req.method !== 'GET' || !file) return send(res, 404, 'Not found', 'text/plain');
  try {
    const data = await fs.readFile(path.join(ROOT, file));
    send(res, 200, data, TYPES[path.extname(file)]);
  } catch {
    send(res, 404, 'Not found', 'text/plain');
  }
});

server.listen(PORT, () => {
  console.log(`🖍️  Draw & Learn is running at http://localhost:${PORT}`);
  if (!process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_AUTH_TOKEN) {
    console.log('⚠️  ANTHROPIC_API_KEY is not set, so "What did I draw?" will not work yet.');
  }
});
