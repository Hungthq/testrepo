// Vercel Function: POST /api/look
import { look } from '../lib/look.js';

// Claude usually answers in a few seconds; leave room for slow moments.
export const maxDuration = 60;

export async function POST(request) {
  let body;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: 'Bad request' }, { status: 400 });
  }
  const result = await look(body, request.headers.get('x-family-code'));
  return Response.json(result.body, { status: result.status, headers: { 'Cache-Control': 'no-store' } });
}
