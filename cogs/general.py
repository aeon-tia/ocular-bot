"""Cog storing commands for general use."""
from typing import Self

import discord
from discord.ext import commands

from src.ocular.operations import DataBase


async def get_mount_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    mounts = await database.list_item_names(
        expansion=ctx.options["expansion"],
    )
    return mounts  # noqa: RET504


async def get_expansion_names(ctx: discord.AutocompleteContext) -> list[str]:  # noqa: ARG001
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    expansions = await database.list_expansions()
    return expansions  # noqa: RET504


class General(commands.Cog):
    """Class to hold general use commands."""

    def __init__(self: Self, bot: discord.Bot) -> None:
        """Store general use commands."""
        self.bot = bot


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(General(bot))
