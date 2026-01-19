#!/usr/bin/env python3
"""Test Telegram notifications."""

import asyncio
from decimal import Decimal

from rarb.notifications.telegram import TelegramNotifier


async def test_telegram():
    """Test sending a Telegram notification."""
    notifier = TelegramNotifier()
    
    print("Testing Telegram notification...")
    print(f"Bot token: {'set' if notifier.bot_token else 'not set'}")
    print(f"Chat ID: {'set' if notifier.chat_id else 'not set'}")
    
    if not notifier.bot_token or not notifier.chat_id:
        print("\n❌ Telegram credentials not configured in .env")
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        return
    
    print("\nSending test notification...")
    
    # Test startup notification
    success = await notifier.notify_startup(mode="TEST", markets=10)
    
    if success:
        print("✅ Test notification sent successfully!")
        print("Check your Telegram chat for the message.")
        
        # Test arbitrage notification
        await asyncio.sleep(1)
        await notifier.notify_arbitrage(
            market="Test Market - Will Bitcoin hit $100k?",
            yes_ask=Decimal("0.51"),
            no_ask=Decimal("0.48"),
            combined=Decimal("0.99"),
            profit_pct=Decimal("0.01"),
        )
        print("✅ Arbitrage notification sent!")
        
    else:
        print("❌ Failed to send notification")
        print("Check your bot token and chat ID")
    
    await notifier.close()


if __name__ == "__main__":
    asyncio.run(test_telegram())
