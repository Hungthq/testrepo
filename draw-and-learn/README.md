# 🖍️ Draw & Learn

A drawing app for kids. **Your child draws anything they like and taps ✨ "What did I draw?".** Claude looks at the picture and guesses what it is, then:

- 🎉 makes the drawing jump to life under a shower of emoji
- 💬 says something kind about *their* drawing ("I love the pointy ears!")
- 🧠 shares 3 fun facts, 📖 tells a short story starring their drawing, 😂 tells a joke, and 🔊 makes its sound ("Meow meow!")
- 🎨 suggests what to draw next ("Can you draw a ball of yarn for the cat?")

Everything is read aloud, so kids who can't read yet can use it too. If Claude guesses wrong, kids tap one of its other guesses, or type or 🎤 say what they drew, and it tries again. Every guessed drawing is kept in 🖼️ **My Gallery** on that device. The **EN/VI** button switches between English and Tiếng Việt.

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

### Settings

| Variable | Default | What it does |
|---|---|---|
| `ANTHROPIC_API_KEY` | (required) | Your Claude API key |
| `PORT` | `3000` | Port the app runs on |
| `CLAUDE_MODEL` | `claude-opus-5-5` | Which Claude model looks at the drawings |

**Cost:** each ✨ press is one Claude request with a small image, roughly 1–3 US cents with the default model. The server asks for low effort to keep answers quick. If Claude's safety checks ever decline a request, it's automatically retried on Anthropic's recommended fallback model (`fallbacks: "default"`).

**Privacy:** drawings are sent to the Claude API only when the ✨ button is pressed. The gallery is stored in the browser on that device. The app never asks for names or other personal details.

## How it works

| File | What it does |
|---|---|
| `index.html`, `style.css` | The page and its kid-friendly styling (works on phones and tablets) |
| `app.js` | Drawing tools, tracing lessons, speech, the ✨ result card and the gallery |
| `server.js` | Serves the app and handles `POST /api/look`. It crops the drawing, sends it to Claude with a kid-safe prompt, and gets back structured JSON (thing, emoji, facts, story, joke, sound, challenge, other guesses) |
