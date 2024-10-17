"""Cog storing commands for general use."""

import logging
from typing import Self

import discord
import polars as pl
from discord.ext import commands

from src.ocular.operations import DataBase

logger = logging.getLogger("discord")


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

    @discord.slash_command(name="ocular", description="Confirm the bot is responsive")
    async def ocular(self: Self, ctx: discord.ApplicationContext) -> None:
        """Check if the bot responds."""
        logger.info("/ocular invoked by %s", ctx.author.name)
        await ctx.respond("We stand together!")
        logger.info("/ocular OK")

    @discord.slash_command(
        name="addme",
        description="Add yourself to the bot's user list",
    )
    @discord.option(
        "name",
        type=str,
        description="Name to give yourself in the database",
    )
    async def addme(self: Self, ctx: discord.ApplicationContext, name: str) -> None:
        """Add user to the user table."""
        logger.info("/addme invoked by %s", ctx.author.name)
        database = DataBase()
        # Check if user name is already added
        name_exists = await database.check_user_exists(
            check_col="user_name",
            check_val=name,
        )
        # Check if discord ID is already added
        id_exists = await database.check_user_exists(
            check_col="user_discord_id",
            check_val=ctx.author.id,
        )
        if name_exists:
            logger.warning("User %s already in database, cancelling", name)
            await ctx.send_response(
                content="I already have a user with this name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif id_exists:
            logger.warning(
                "Discord ID %s already in database, cancelling",
                ctx.author.id,
            )
            await ctx.send_response(
                content="I already have a user with your discord ID in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Adding %s to the database as %s", ctx.author.name, name)
            await database.append_new_user(name=name, discord_id=ctx.author.id)
            await database.append_new_status(discord_id=ctx.author.id)
            await ctx.send_response(
                content=f"You have been added as `{name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/addme OK")

    @discord.slash_command(name="userlist", description="List users in the database")
    async def userlist(self: Self, ctx: discord.ApplicationContext) -> None:
        """Get the list of all users in the database."""
        logger.info("/userlist invoked by %s", ctx.author.name)
        database = DataBase()
        user_table = await database.read_table_polars("users")
        user_list = user_table.select("user_name").to_series().to_list()
        embed = discord.Embed(
            title="Users",
            description=f"List of users in the database: \n - {'\n - '.join(user_list)}",  # noqa: E501
            color=discord.Colour.blurple(),
        )
        await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)
        logger.info("/userlist OK")

    @discord.slash_command(name="mountlist", description="List available mount names")
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Expansion to list mounts from",
    )
    async def mountnames(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
    ) -> None:
        """Show a list of available mount names."""
        logger.info("/mountnames invoked by %s", ctx.author.name)
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        embed = discord.Embed(
            title=f"{expansion.capitalize()} mounts",
            description=f"Available mounts are: \n - {'\n - '.join(item_names)}",
            color=discord.Colour.blurple(),
        )
        await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)
        logger.info("/mountnames OK")

    @discord.slash_command(name="addmount", description="Add mounts to your list")
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Mount expansion",
    )
    @discord.option(
        "name",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_mount_names),
        description="Mount name",
    )
    async def addmount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        name: str,
    ) -> None:
        """Add items to a user in the status table."""
        logger.info("/addmount invoked by %s", ctx.author.name)
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        user_id = await database.get_user_from_discord_id(ctx.author.id)
        if len(user_id) == 0:
            logger.warning("User %s not registered, cancelling", ctx.author.name)
            await ctx.send_response(
                content="I don't have you in my database! Add yourself with `/addme`.",
                ephemeral=True,
                delete_after=90,
            )
        elif name not in item_names:
            logger.warning("Mount %s not found in database, cancelling", name)
            await ctx.send_response(
                content=f"I don't have a `{expansion}` mount named `{name}` in my database.",  # noqa: E501
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Adding mount %s for %s", name, ctx.author.name)
            await database.update_user_items(
                action="add",
                user=ctx.author.id,
                item_names=name,
            )
            await ctx.send_response(
                content=f"Added `{name}` to your `{expansion}` mounts.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/addmount OK")

    @discord.slash_command(
        name="removemount",
        description="Remove mounts from your list",
    )
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Mount expansion",
    )
    @discord.option(
        "name",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_mount_names),
        description="Mount name",
    )
    async def removemount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        name: str,
    ) -> None:
        """Add items to a user in the status table."""
        logger.info("/removemount invoked by %s", ctx.author.name)
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        user_id = await database.get_user_from_discord_id(ctx.author.id)
        if len(user_id) == 0:
            logger.warning("User %s not registered, cancelling", ctx.author.name)
            await ctx.send_response(
                content="I don't have you in my database! Add yourself with `/addme`.",
                ephemeral=True,
                delete_after=90,
            )
        elif name not in item_names:
            logger.warning("Mount %s not found in database, cancelling", name)
            await ctx.send_response(
                content=f"I don't have a `{expansion}` mount named `{name}` in my database.",  # noqa: E501
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Removing mount %s from %s", name, ctx.author.name)
            await database.update_user_items(
                user=ctx.author.id,
                action="remove",
                item_names=name,
            )
            await ctx.send_response(
                content=f"Removed `{name}` from your `{expansion}` mounts.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/removemount OK")

    @discord.slash_command(name="mymounts", description="View your mounts")
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Expansion to list mounts from",
    )
    async def mymounts(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
    ) -> None:
        """View list of held and needed mounts."""
        logger.info("/mymounts invoked by %s", ctx.author.name)
        database = DataBase()
        user_id = await database.get_user_from_discord_id(ctx.author.id)
        if len(user_id) == 0:
            logger.warning("User %s not registered, cancelling", ctx.author.name)
            await ctx.send_response(
                content="I don't have you in my database! Add yourself with `/addme`.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            has_mounts = await database.list_user_items(
                user=ctx.author.id,
                check_type="has",
                expansion=expansion,
            )
            needs_mounts = await database.list_user_items(
                user=ctx.author.id,
                check_type="needs",
                expansion=expansion,
            )
            image_urls = {
                "a realm reborn": "https://lds-img.finalfantasyxiv.com/h/-/pnlEUJhVj0vMO7dtJ5psZ84Vvg.jpg",
                "heavensward": "https://lds-img.finalfantasyxiv.com/h/3/uN1BWnRvdTy5nT8izK6G4Hu3cI.jpg",
                "stormblood": "https://lds-img.finalfantasyxiv.com/h/v/CBMATiZFo0BaxDrY2G483WScs4.jpg",
                "shadowbringers": "https://lds-img.finalfantasyxiv.com/h/m/B56bwbNBbqkA9UlbmcZ_BeWIL8.jpg",
                "endwalker": "https://lds-img.finalfantasyxiv.com/h/Q/YW-_Cq8HEN5QOH5PD5w9xF2-YI.jpg",
                "dawntrail": "https://lds-img.finalfantasyxiv.com/h/Z/1Li39bwJmXi701FfGzRL_7LAZg.jpg",
            }
            image_url = image_urls[expansion]
            embed = discord.Embed(
                title=f"{expansion.capitalize()} mounts",
                color=discord.Colour.blurple(),
            )
            embed.add_field(
                name="Have",
                value=f" - {'\n - '.join(has_mounts)}",
                inline=True,
            )
            embed.add_field(
                name="Need",
                value=f" - {'\n - '.join(needs_mounts)}",
                inline=True,
            )
            embed.set_image(url=image_url)
            embed.set_thumbnail(url=ctx.author.avatar)
            await ctx.respond(embed=embed)
        logger.info("/mymounts OK")

    @discord.slash_command(
        name="mostneeded",
        description="Display the top n most needed mounts",
    )
    @discord.option("n", type=int, description="Number of results to output")
    async def mostneeded(
        self: Self,
        ctx: discord.ApplicationContext,
        n: int,
    ) -> None:
        """Display the top n most needed mounts."""
        logger.info("/mostneeded invoked by %s", ctx.author.name)
        database = DataBase()
        needed_mounts = await database.summarize_needed_mounts()
        output = needed_mounts[0:n]
        item_expansion_list = output.select("item_expac").to_series().to_list()
        item_name_list = output.select("item_name").to_series().to_list()
        item_count_list = (
            output.select("need_count").cast(pl.String).to_series().to_list()
        )
        embed = discord.Embed(
            title="Most commonly needed mounts",
            color=discord.Colour.blurple(),
        )
        embed.add_field(
            name="Expansion",
            value=f"{'\n '.join(item_expansion_list)}",
            inline=True,
        )
        embed.add_field(
            name="Mount",
            value=f"{'\n '.join(item_name_list)}",
            inline=True,
        )
        embed.add_field(
            name="Needed by",
            value=f"{'\n '.join(item_count_list)}",
            inline=True,
        )
        await ctx.send_response(embed=embed, ephemeral=True)
        logger.info("/mostneeded OK")


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(General(bot))
