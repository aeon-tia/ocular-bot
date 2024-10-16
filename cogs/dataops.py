"""Cog storing commands for modifying the database."""
from typing import Self

import discord
from discord.ext import commands


class DataOps(commands.Cog):
    """Class to hold database modification commands."""

    def __init__(self: Self, bot: discord.Bot) -> None:
        """Store database modification commands."""
        self.bot = bot


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(DataOps(bot))
