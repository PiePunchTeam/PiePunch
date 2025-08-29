# PiePunch
Code for PiePunch UFC site
# PiePunch (Ultimate Fight Hub Rebuild)

Ultimate Fight Hub is a UFC-focused website for previewing upcoming cards, in-depth matchups, live social polls on fights, a fight simulator using standardized stats, and prop bet recommendations.

## Structure
- `/scraper`: Python scripts for scraping and AI agents (core scraper, defensive stats, derived stats, badges).
- `/backend`: Node.js server for API endpoints (previews, matchups, polls, simulator, prop bets).
- `/firestore`: Scripts for Firestore uploads.
- `/config`: Environment variables (e.g., .env).
- `/docs`: Documentation (optional).
- `.github/workflows`: GitHub Actions for weekly automation.

## Setup
1. Install Python dependencies: `pip install -r requirements.txt`.
2. Install Node.js dependencies: `npm install`.
3. Set up Firebase: Add `firebase-adminsdk.json` to `/config` (not committed; use GitHub Secrets).
4. Run locally: `python scraper/core_scraper.py` for scraping, `node backend/index.js` for backend.
5. Deploy: Push to `main` for Vercel auto-deploy.

## Automation
- Weekly scrape via GitHub Actions: Updates only new fights (~12-15/week) to Firestore.

## Firestore Collections
- `Events`: Event metadata.
- `Fighters`: Per-fighter profiles with 52 standardized stats (per-15-minute rates where applicable).
- `Past Fights`: Historical fight data.
- `Upcoming Events`: Future events.
- `Upcoming Fights`: Scheduled fights.

Contact for issues: [Your email or GitHub].
