# 🖍️ Draw & Learn

A drawing app for kids. **Your child draws anything they like and taps ✨ "What did I draw?".** Claude looks at the picture and guesses what it is, then:

- 🎉 makes the drawing jump to life under a shower of emoji
- 💬 says something kind about *their* drawing ("I love the pointy ears!")
- 📖 tells a **fun story** starring their drawing as the hero, and reads it aloud
- 🎵 writes a **short song** about it to a nursery tune everyone knows (Twinkle Twinkle, Row Row Row Your Boat, Mary Had a Little Lamb or London Bridge). Tap 🎤 **Sing along** to play the tune while each line lights up, karaoke-style
- 🧠 shares 3 fun facts, 😂 tells a joke, and 🔊 makes its sound ("Meow meow!")
- 🎨 suggests what to draw next ("Can you draw a ball of yarn for the cat?")

Everything is read aloud, so kids who can't read yet can use it too. If Claude guesses wrong, kids tap one of its other guesses, or type or 🎤 say what they drew, and it tries again. Every guessed drawing, with its story and song, is kept in 🖼️ **My Gallery** on that device.

The app also has tracing lessons: 🔤 ABC, 🔢 123 and 🔷 Shapes, with stars for good tracing. These work without the server.

## Setup (for grown-ups)

The ✨ button uses the Claude API. The key stays on a small server on your computer and never goes to the kids' tablet.

1. Install [Node.js](https://nodejs.org) 18 or newer.
2. Get an API key at <https://console.anthropic.com>.
3. Start the app:

   ```bash
   cd draw-and-learn
   npm install
   ANTHROPIC_API_KEY=sk-ant-... npm start
   ```

   On Windows PowerShell: `$env:ANTHROPIC_API_KEY="sk-ant-..."; npm start`

4. Open <http://localhost:3000>. On a tablet or phone on the same Wi-Fi, open `http://<your-computer's-IP>:3000`, then use "Add to Home Screen" so it opens like an app.

### Deploying to Vercel

The app also runs on [Vercel](https://vercel.com): the page is served as static files and `api/look.js` runs as a Vercel Function.

1. Import the GitHub repo in Vercel and set **Root Directory** to `draw-and-learn`.
2. In the project's **Settings → Environment Variables**, add `ANTHROPIC_API_KEY` (mark it Sensitive). Adding a `FAMILY_CODE` is strongly recommended too (see below).
3. Redeploy. Pushes to `main` deploy automatically after that.
4. Open `/api/health?check=1` on your site to confirm the key works: look for `"claude":{"ok":true}`.

### Settings

| Variable | Default | What it does |
|---|---|---|
| `ANTHROPIC_API_KEY` | (required) | Your Claude API key |
| `ANTHROPIC_WORKSPACE_ID` | (none) | Only needed for organization-wide service keys (`sk-ant-svc-...`): the workspace to use, like `wrkspc_...`. Regular API keys (`sk-ant-api03-...`) don't need it. |
| `PORT` | `3000` | Port the app runs on |
| `CLAUDE_MODEL` | `claude-opus-5-5` | Which Claude model looks at the drawings |
| `FAMILY_CODE` | (none) | Optional password. When set, each device asks a grown-up for the code once and remembers it, so strangers who find the URL can't spend your API credits. Recommended on Vercel. |

**Cost:** each ✨ press is one Claude request with a small image, roughly 1–3 US cents with the default model. The server asks for low effort to keep answers quick. If Claude's safety checks ever decline a request, it's automatically retried on Anthropic's recommended fallback model (`fallbacks: "default"`).

**Privacy:** drawings are sent to the Claude API only when the ✨ button is pressed. The gallery is stored in the browser on that device. The app never asks for names or other personal details.

## How it works

| File | What it does |
|---|---|
| `index.html`, `style.css` | The page and its kid-friendly styling (works on phones and tablets) |
| `app.js` | Drawing tools, tracing lessons, speech, the ✨ result card, the sing-along player and the gallery |
| `lib/look.js` | Sends the cropped drawing to Claude with a kid-safe prompt and gets back structured JSON (thing, emoji, story, song lyrics and tune, facts, joke, sound, challenge, other guesses) |
| `server.js` | Local server: serves the app and handles `POST /api/look` using `lib/look.js` |
| `api/look.js`, `api/health.js` | The same endpoints as Vercel Functions |
