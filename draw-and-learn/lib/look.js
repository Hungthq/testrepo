// Shared "What did I draw?" logic, used by both the local server (server.js)
// and the Vercel Function (api/look.js).

import Anthropic from '@anthropic-ai/sdk';

const MODEL = process.env.CLAUDE_MODEL || 'claude-opus-5-5';

let client = null;

// API keys never contain spaces, line breaks or quotes, but all three sneak in
// easily when a key is pasted on a phone, so clean them off.
export function apiKey() {
  return String(process.env.ANTHROPIC_API_KEY || '').replace(/[\s"'`]/g, '');
}

// Organization-wide keys (sk-ant-svc-...) must say which workspace to use.
function workspaceId() {
  return String(process.env.ANTHROPIC_WORKSPACE_ID || '').replace(/[\s"'`]/g, '');
}

function getClient() {
  const workspace = workspaceId();
  client = client || new Anthropic({
    apiKey: apiKey() || null,
    defaultHeaders: workspace ? { 'anthropic-workspace-id': workspace } : undefined,
  });
  return client;
}

// A safe description of the key (never the key itself) for /api/health.
export function describeKey() {
  const key = apiKey();
  const raw = String(process.env.ANTHROPIC_API_KEY || '');
  return {
    set: key.length > 0,
    length: key.length,
    starts_with: key.slice(0, 12),
    looks_like_api_key: key.startsWith('sk-ant-api'),
    is_service_key: key.startsWith('sk-ant-svc'),
    workspace_id: workspaceId() || null,
    had_spaces_quotes_or_newlines: raw !== key,
  };
}

// Asks Anthropic whether the key works, without using any tokens.
export async function checkKey() {
  if (!apiKey()) return { ok: false, problem: 'ANTHROPIC_API_KEY is not set in this deployment' };
  try {
    await getClient().models.retrieve(MODEL);
    return { ok: true, model: MODEL };
  } catch (err) {
    return {
      ok: false,
      status: err.status ?? null,
      type: err.error?.error?.type ?? err.name,
      message: err.error?.error?.message ?? err.message,
    };
  }
}

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

  const response = await getClient().beta.messages.create({
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

// When FAMILY_CODE is set, only devices that know the code can use Claude
// (so a public URL can't spend your API credits).
export function checkFamilyCode(code) {
  const expected = process.env.FAMILY_CODE;
  return !expected || String(code || '').trim().toLowerCase() === expected.trim().toLowerCase();
}

// Takes the parsed request body and returns { status, body } for the response.
export async function look(body, familyCode) {
  if (!checkFamilyCode(familyCode)) {
    return { status: 401, body: { error: 'code' } };
  }
  const image = String(body?.image || '').replace(/^data:image\/png;base64,/, '');
  if (!image || !/^[A-Za-z0-9+/=]+$/.test(image)) {
    return { status: 400, body: { error: 'Missing drawing' } };
  }
  const hint = String(body.hint || '').slice(0, 80).replace(/["\n\r]/g, ' ').trim();

  try {
    return { status: 200, body: await askClaude({ image, hint }) };
  } catch (err) {
    const detail = err.error?.error?.message ?? err.message;
    if (err instanceof Anthropic.AuthenticationError) {
      console.error('Claude rejected the API key:', detail);
      return { status: 500, body: { error: 'setup', detail: `Claude said: ${detail} (${err.status})` } };
    }
    if (err instanceof Anthropic.RateLimitError) {
      return { status: 429, body: { error: 'busy' } };
    }
    if (err instanceof Anthropic.APIConnectionError) {
      console.error('Could not reach the Claude API:', err.message);
      return { status: 503, body: { error: 'offline' } };
    }
    if (!apiKey()) {
      console.error('ANTHROPIC_API_KEY is not set.');
      return { status: 500, body: { error: 'setup', detail: 'ANTHROPIC_API_KEY is not set in this deployment.' } };
    }
    console.error('Look failed:', err.status || '', detail);
    return {
      status: err.status === 422 ? 422 : 500,
      body: { error: 'failed', detail: err.status ? `Claude said: ${detail} (${err.status})` : detail },
    };
  }
}
