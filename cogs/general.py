"""Cog storing commands for general use."""
from typing import Self

import discord
from discord.ext import commands


class General(commands.Cog):
    """Class to hold general use commands."""

    def __init__(self: Self, bot: discord.Bot) -> None:
        """Store general use commands."""
        self.bot = bot


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(General(bot))
