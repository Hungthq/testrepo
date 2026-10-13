// Vercel Function: GET /api/health
export function GET() {
  return Response.json({ ok: true });
}
