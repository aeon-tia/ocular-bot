"""Cog storing commands for admin use only."""
from typing import Self

import discord
from discord.ext import commands


class AdminOnly(commands.Cog):
    """Class to hold admin only commands."""

    def __init__(self: Self, bot: discord.bot) -> None:
        """Store admin only commands."""
        self.bot = bot


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(AdminOnly(bot))
