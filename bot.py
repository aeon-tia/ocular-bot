"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import logging
import os
from logging.handlers import RotatingFileHandler

import discord
from dotenv import load_dotenv

log_formatter = logging.Formatter(
    "{asctime} | {levelname:8s} | {funcName}:{lineno} {message}",
    style="{",
)
log_handler = RotatingFileHandler(
    filename="bot.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=2,
)
log_handler.setFormatter(log_formatter)
bot_log = logging.getLogger()
bot_log.setLevel(logging.INFO)
bot_log.addHandler(log_handler)

bot = discord.Bot()


def main() -> None:
    """Run program."""
    bot_log.info("Launching Ocular")
    load_dotenv()
    bot.run(os.getenv("TOKEN"))


if __name__ == "__main__":
    main()
