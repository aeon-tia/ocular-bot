"""Cog storing commands for admin use only."""

import logging
from typing import Self

import discord
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


class AdminOnly(commands.Cog):
    """Class to hold admin only commands."""

    def __init__(self: Self, bot: discord.bot) -> None:
        """Store admin only commands."""
        self.bot = bot

    @discord.slash_command(
        name="adminaddmount",
        description="(Admin only) Add mounts for a user",
    )
    @commands.has_role(547835267394830348)
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Mount expansion",
    )
    @discord.option(
        "mount_name",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_mount_names),
        description="Mount name",
    )
    @discord.option("user_name", type=str, description="User to add mounts for")
    async def adminaddmount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        mount_name: str,
        user_name: str,
    ) -> None:
        """Add items to a user other than the author in the status table.

        Parameters
        ----------
        ctx : discord.ApplicationContext
            Discord context. Used for interacting with the command
            invoker.
        expansion : str
            Name of the FFXIV expansion to add mounts from.
        mount_name : str
            Name of the mount to add.
        user_name : str
            Name of the user to add mounts for.

        """
        logger.info("/adminaddmount invoked by %s", ctx.author.name)
        database = DataBase()
        user_did = await database.get_user_discord_id(user_name)
        item_names = await database.list_item_names(expansion)
        if len(user_did) == 0:
            logger.warning("User %s not found, cancelling", user_name)
            await ctx.send_response(
                content=f"I don't have a user named `{user_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif mount_name not in item_names:
            logger.warning("Mount %s not found, cancelling", mount_name)
            await ctx.send_response(
                content=f"I don't have a `{expansion}` mount named `{mount_name}` in my database.",  # noqa: E501
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Adding mount %s for user %s", mount_name, user_name)
            await database.update_user_items(
                action="add",
                user=user_did[0],
                item_names=mount_name,
            )
            await ctx.send_response(
                content=f"Added `{expansion}` mount `{mount_name}` for `{user_name}`",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/adminaddmount OK")

    @discord.slash_command(
        name="adminremovemount",
        description="(Admin only) Remove mounts from a user",
    )
    @commands.has_role(547835267394830348)
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Mount expansion",
    )
    @discord.option(
        "mount_name",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_mount_names),
        description="Mount name",
    )
    @discord.option("user_name", type=str, description="User to remove mount from")
    async def adminremovemount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        mount_name: str,
        user_name: str,
    ) -> None:
        """Add items to a user other than the author in the status table.

        Parameters
        ----------
        ctx : discord.ApplicationContext
            Discord context. Used for interacting with the command
            invoker.
        expansion : str
            Name of the FFXIV expansion to remove mounts from.
        mount_name : str
            Name of the mount to remove.
        user_name : str
            Name of the user to remove mounts for.

        """
        logger.info("/adminremovemount invoked by %s", ctx.author.name)
        database = DataBase()
        user_did = await database.get_user_discord_id(user_name)
        item_names = await database.list_item_names(expansion)
        if len(user_did) == 0:
            logger.warning("User %s not found, cancelling", user_name)
            await ctx.send_response(
                content=f"I don't have a user named `{user_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif mount_name not in item_names:
            logger.warning("Mount %s not found, cancelling", mount_name)
            await ctx.send_response(
                content=f"I don't have a `{expansion}` mount named `{mount_name}` in my database.",  # noqa: E501
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Removing mount %s from user %s", mount_name, user_name)
            await database.update_user_items(
                action="remove",
                user=user_did[0],
                item_names=mount_name,
            )
            await ctx.send_response(
                content=f"Removed `{expansion}` mount `{mount_name}` from `{user_name}`",  # noqa: E501
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/adminremovemount OK")

    @discord.slash_command(
        name="adminusermounts",
        description="(Admin only) View another users mounts",
    )
    @commands.has_role(547835267394830348)
    @discord.option("user_name", type=str, description="User to check mounts for")
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Expansion to list mounts from",
    )
    async def adminusermounts(
        self: Self,
        ctx: discord.ApplicationContext,
        user_name: str,
        expansion: str,
    ) -> None:
        """View list of held and needed mounts.

        Parameters
        ----------
        ctx : discord.ApplicationContext
            Discord context. Used for interacting with the command
            invoker.
        user_name : str
            Name of user to display mounts for.
        expansion : str
            Name of the FFXIV expansion to display mounts from.

        """
        logger.info("/adminusermounts invoked by %s", ctx.author.name)
        database = DataBase()
        user_did = await database.get_user_discord_id(user_name)
        if len(user_did) == 0:
            logger.warning("User %s not found, cancelling", user_name)
            await ctx.send_response(
                content=f"I don't have a user named `{user_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Listing mounts held by %s", user_name)
            has_mounts = await database.list_user_items(
                user=user_did[0],
                check_type="has",
                expansion=expansion,
            )
            needs_mounts = await database.list_user_items(
                user=user_did[0],
                check_type="needs",
                expansion=expansion,
            )
            embed = discord.Embed(
                title=f"{expansion.capitalize()} mounts for `{user_name}`",
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
            await ctx.send_response(embed=embed, ephemeral=True)
        logger.info("/adminusermounts OK")


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(AdminOnly(bot))
