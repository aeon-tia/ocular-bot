"""Ocular bot - Discord bot for tracking FFXIV mount progress."""
import logging
import os
import platform
from pathlib import Path
from typing import Self

import discord
import yaml
from discord.ext import commands
from discord.ext.commands import Context
from dotenv import load_dotenv

from ocular import Database

# Set config
with Path.open("./config.yml") as file:
    config = yaml.safe_load(file)

# Set logging
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Set bot intents
intents = discord.Intents.default()


class DiscordBot(commands.Bot):
    """Commands for interfacing with the bot."""

    def __init__(self: Self) -> None:
        """Interface for ocular bot."""
        super().__init__(command_prefix="/", intents=intents, help_command=None)

    async def init_db(self: Self) -> None:
        """Create the bot database."""
        database = Database()
        await database.init_tables()

    async def get_id(self: Self, user: discord.User) -> None:
        """Get the discord ID of the user invoking a command.

        Parameters
        ----------
        user : discord.User
            API object with user information.

        """
        return user.id


load_dotenv()

bot = DiscordBot()
bot.run(os.getenv("TOKEN"))
