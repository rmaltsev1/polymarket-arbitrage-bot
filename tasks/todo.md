# Telegram Notifications Implementation

## Plan

### Tasks
- [x] Create TelegramNotifier class in src/rarb/notifications/telegram.py
- [x] Update src/rarb/notifications/__init__.py to export TelegramNotifier
- [x] Update src/rarb/notifications/slack.py get_notifier() to support both Slack and Telegram
- [x] Test that notifications work with Telegram credentials in .env

## Design

The implementation will:
1. Create a new `TelegramNotifier` class that mirrors the `SlackNotifier` interface
2. Use the Telegram Bot API to send messages (simple HTTP POST requests, no extra dependencies)
3. Support the same notification methods: `notify_arbitrage`, `notify_trade`, `notify_error`, `notify_startup`, `notify_shutdown`, `notify_daily_summary`
4. Modify `get_notifier()` to check for Telegram credentials first, then fall back to Slack
5. Keep changes minimal - only add new file and small modifications to existing code

## Review

### Changes Made

1. **Created `src/rarb/notifications/telegram.py`**
   - Implemented `TelegramNotifier` class with all notification methods
   - Uses Telegram Bot API (https://api.telegram.org) via aiohttp
   - Formats messages with HTML for better readability
   - Includes emojis for visual clarity (🎯 for arbitrage, ✅ for trades, 🚨 for errors, etc.)

2. **Updated `src/rarb/notifications/__init__.py`**
   - Exported `TelegramNotifier` alongside `SlackNotifier`

3. **Updated `src/rarb/notifications/slack.py`**
   - Modified `get_notifier()` to check for Telegram credentials first
   - Falls back to Slack if Telegram not configured
   - Maintains backward compatibility with existing Slack users

4. **Created `scripts/test_telegram.py`**
   - Test script to verify Telegram notifications work
   - Sends test startup and arbitrage notifications

### Testing

✅ Successfully tested Telegram notifications
- Test messages sent to configured Telegram chat
- Both startup and arbitrage notifications working correctly
- No new dependencies required (uses existing aiohttp)

### Impact

- **Minimal code changes** - Only added one new file and modified two existing files
- **Backward compatible** - Existing Slack users unaffected
- **Auto-detection** - Bot automatically uses Telegram if credentials are set in .env
- **No breaking changes** - All existing code continues to work as before

