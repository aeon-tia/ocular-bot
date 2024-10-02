"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import os

import discord
from dotenv import load_dotenv

from src.ocular.operations import DataBase

load_dotenv()
bot = discord.Bot()


@bot.event
async def on_ready() -> None:
    """Print status message when bot comes online."""
    print(f"{bot.user} is ready and online!")  # noqa: T201


@bot.slash_command(name="squeak", description="Confirm the bot is responsive.")
async def squeak(ctx: discord.ApplicationContext) -> None:
    """Check if the bot responds."""
    await ctx.respond("Sqrk!")


@bot.slash_command(name="initdb", description="Initialize database.")
async def init_db() -> None:
    """Create the bot database."""
    database = DataBase()
    await database.init_tables()


bot.run(os.getenv("TOKEN"))
