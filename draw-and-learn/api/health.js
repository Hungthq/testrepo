// Vercel Function: GET /api/health
// Add ?check=1 to also test the Claude API key (the key itself is never shown).
import { checkKey, describeKey } from '../lib/look.js';

export async function GET(request) {
  const body = { ok: true };
  if (new URL(request.url).searchParams.has('check')) {
    body.key = describeKey();
    body.claude = await checkKey();
  }
  return Response.json(body, { headers: { 'Cache-Control': 'no-store' } });
}
