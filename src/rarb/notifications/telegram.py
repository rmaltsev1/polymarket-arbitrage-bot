"""Telegram bot notifications."""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Optional

import aiohttp

from rarb.config import get_settings
from rarb.utils.logging import get_logger

log = get_logger(__name__)


class TelegramNotifier:
    """Send notifications to Telegram via Bot API."""

    def __init__(
        self, bot_token: Optional[str] = None, chat_id: Optional[str] = None
    ) -> None:
        settings = get_settings()
        self.bot_token = bot_token or settings.telegram_bot_token
        self.chat_id = chat_id or settings.telegram_chat_id
        self._session: Optional[aiohttp.ClientSession] = None

        if self.bot_token:
            self.api_url = f"https://api.telegram.org/bot{self.bot_token}"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def send(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send a message to Telegram."""
        if not self.bot_token or not self.chat_id:
            log.debug("Telegram bot not configured, skipping notification")
            return False

        try:
            session = await self._get_session()
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
            }

            async with session.post(
                f"{self.api_url}/sendMessage", json=payload
            ) as resp:
                if resp.status == 200:
                    log.debug("Telegram notification sent")
                    return True
                else:
                    error_text = await resp.text()
                    log.warning(
                        "Telegram notification failed", status=resp.status, error=error_text
                    )
                    return False

        except Exception as e:
            log.error("Failed to send Telegram notification", error=str(e))
            return False

    async def notify_arbitrage(
        self,
        market: str,
        yes_ask: Decimal,
        no_ask: Decimal,
        combined: Decimal,
        profit_pct: Decimal,
    ) -> bool:
        """Send arbitrage opportunity notification."""
        message = f"""
🎯 <b>Arbitrage Detected</b>

<b>Market:</b> {market[:100]}
<b>Profit:</b> +{float(profit_pct) * 100:.2f}%
<b>YES Ask:</b> ${float(yes_ask):.4f}
<b>NO Ask:</b> ${float(no_ask):.4f}
<b>Combined:</b> ${float(combined):.4f}

<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>
"""
        return await self.send(message.strip())

    async def notify_trade(
        self,
        platform: str,
        market: str,
        side: str,
        outcome: str,
        price: Decimal,
        size: Decimal,
        status: str = "executed",
    ) -> bool:
        """Send trade execution notification."""
        emoji = "✅" if status == "executed" else "⚠️"
        
        message = f"""
{emoji} <b>Trade {status.title()}</b>

<b>Platform:</b> {platform}
<b>Action:</b> {side.upper()} {outcome.upper()}
<b>Price:</b> ${float(price):.4f}
<b>Size:</b> ${float(size):.2f}
<b>Market:</b> {market[:80]}

<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>
"""
        return await self.send(message.strip())

    async def notify_error(self, error: str, context: Optional[str] = None) -> bool:
        """Send error notification."""
        message = f"""
🚨 <b>Error Alert</b>

<code>{error[:500]}</code>
"""
        if context:
            message += f"\n<b>Context:</b> {context[:200]}"
        
        message += f"\n\n<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        
        return await self.send(message.strip())

    async def notify_startup(self, mode: str, markets: int = 0) -> bool:
        """Send bot startup notification."""
        settings = get_settings()

        message = f"""
🚀 <b>rarb Bot Started</b>

<b>Mode:</b> {mode}
<b>Min Profit:</b> {settings.min_profit_threshold * 100:.1f}%
<b>Max Position:</b> ${settings.max_position_size}
<b>Markets:</b> {markets}

<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>
"""
        return await self.send(message.strip())

    async def notify_shutdown(self, reason: str = "normal") -> bool:
        """Send bot shutdown notification."""
        message = f"""
🛑 <b>rarb Bot Shutting Down</b>

<b>Reason:</b> {reason}

<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>
"""
        return await self.send(message.strip())

    async def notify_daily_summary(
        self,
        trades: int,
        volume: float,
        profit: float,
        alerts: int,
    ) -> bool:
        """Send daily summary notification."""
        message = f"""
📊 <b>Daily Summary</b>

<b>Trades:</b> {trades}
<b>Volume:</b> ${volume:.2f}
<b>Profit:</b> ${profit:.2f}
<b>Alerts:</b> {alerts}

<i>{datetime.now().strftime('%Y-%m-%d')}</i>
"""
        return await self.send(message.strip())


# Global notifier instance
_telegram_notifier: Optional[TelegramNotifier] = None


def get_telegram_notifier() -> TelegramNotifier:
    """Get the global Telegram notifier instance."""
    global _telegram_notifier
    if _telegram_notifier is None:
        _telegram_notifier = TelegramNotifier()
    return _telegram_notifier
