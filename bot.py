"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import logging
import os
import platform
from pathlib import Path
from typing import Self

import discord
import yaml
from discord.ext import commands
from dotenv import load_dotenv

from src.ocular.operations import DataBase

load_dotenv()
bot = discord.Bot()


async def get_id(message: discord.Message) -> None:
    """Get the discord ID of the user invoking a command.

    Parameters
    ----------
    message : discord.Message
        API object with message and sender information.

    """
    return message.author.id


@bot.slash_command(name="initdb", description="Initialize database.")
async def init_db() -> None:
    """Create the bot database."""
    database = DataBase()
    await database.init_tables()


bot.run(os.getenv("TOKEN"))
