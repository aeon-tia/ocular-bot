"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import logging
import os
from logging.handlers import RotatingFileHandler

import discord
from dotenv import load_dotenv

logger = logging.getLogger("discord")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "{asctime} | {levelname:8s} | {funcName}:{lineno} {message}",
    style="{",
)
handler = RotatingFileHandler(
    filename="bot.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=2,
)
logger.setFormatter(formatter)
logger.addHandler(handler)

bot = discord.Bot()


def main() -> None:
    """Run program."""
    logger.info("Launching Ocular")
    load_dotenv()
    cog_list = ["general", "adminonly", "dataops"]
    for cog in cog_list:
        bot.load_extension(f"cogs.{cog}")
    bot.run(os.getenv("TOKEN"))


if __name__ == "__main__":
    main()
