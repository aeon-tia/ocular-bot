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
async def oinit_db(ctx: discord.ApplicationContext) -> None:
    """Create the bot database."""
    database = DataBase()
    await database.init_tables()
    await ctx.respond("Database intialized.")


@bot.slash_command(
    name="oadd",
    description="Add mounts to your list. Option `kind` must be 'trials' or 'raids'. Option `names` must be a single mount name or a comma-separated list of mount names, e.g., 'ifrit,titan'."
)
async def oadd(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    names: discord.Option(str),
) -> None:
    """Add items to a user in the status table."""
    database = DataBase()
    await database.update_user_items(
        user=ctx.author.id,
        action="add",
        item_kind=kind,
        item_names=names,
    )
    await ctx.respond(f"Added mounts: {names}")


@bot.slash_command(
    name="oremove",
    description="Remove mounts from your list. Option `kind` must be 'trials' or 'raids'. Option `names` must be a single mount name or a comma-separated list of mount names, e.g., 'ifrit,titan'."
)
async def oremove(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    names: discord.Option(str),
) -> None:
    """Add items to a user in the status table."""
    database = DataBase()
    await database.update_user_items(
        user=ctx.author.id,
        action="remove",
        item_kind=kind,
        item_names=names,
    )
    await ctx.respond(f"Removed mounts: {names}")


bot.run(os.getenv("TOKEN"))
