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
    print(f"{bot.user} is ready!")  # noqa: T201


@bot.slash_command(name="oping", description="Confirm the bot is responsive.")
async def oping(ctx: discord.ApplicationContext) -> None:
    """Check if the bot responds."""
    await ctx.respond("I'm online!")


@bot.slash_command(name="oinitdb", description="Initialize database.")
async def oinit_db() -> None:
    """Create the bot database."""
    database = DataBase()
    await database.init_tables()


bot.run(os.getenv("TOKEN"))
