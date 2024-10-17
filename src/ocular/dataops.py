"""Cog storing commands for modifying the database."""

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


class DataOps(commands.Cog):
    """Class to hold database modification commands."""

    def __init__(self: Self, bot: discord.Bot) -> None:
        """Store database modification commands."""
        self.bot = bot

    @discord.slash_command(
        name="dbcreatemount",
        description="(Admin only) Create a new mount in the database",
    )
    @commands.has_role(547835267394830348)
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
        description="Mount name to create",
    )
    async def dbcreatemount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        name: str,
    ) -> None:
        """Create new mounts in the database."""
        logger.info("/dbcreatemount invoked by %s", ctx.author.name)
        database = DataBase()
        item_id = await database.get_item_id(name)
        already_exists = len(item_id) != 0
        if already_exists:
            logger.warning(
                "Expansion %s mount %s already in database, cancelling",
                expansion,
                name,
            )
            await ctx.send_response(
                content="I already have a mount with this name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Adding expansion %s mount %s to database", expansion, name)
            await database.add_new_item(expansion, name)
            await ctx.send_response(
                content=f"Created `{expansion}` mount `{name}`",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/dbcreatemount OK")

    @discord.slash_command(
        name="dbdeletemount",
        description="(Admin only) Delete a mount from the database",
    )
    @commands.has_role(547835267394830348)
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
        description="Mount name to delete",
    )
    async def dbdeletemount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        name: str,
    ) -> None:
        """Delete a mount from the database."""
        logger.info("/dbdeletemount invoked by %s", ctx.author.name)
        database = DataBase()
        item_id = await database.get_item_id(name)
        no_match = len(item_id) == 0
        if no_match:
            logger.warning(
                "Expansion %s mount %s not found, cancelling",
                expansion,
                name,
            )
            await ctx.send_response(
                content="I don't have a mount with this name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Deleting expansion %s mount %s", expansion, name)
            await database.delete_item(name)
            await ctx.send_response(
                content=f"Deleted `{expansion}` mount `{name}` from the database.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/dbdeletemount OK")

    @discord.slash_command(
        name="dbrenamemount",
        description="(Admin only) Rename a mount in the database",
    )
    @commands.has_role(547835267394830348)
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Mount expansion",
    )
    @discord.option(
        "from_name",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_mount_names),
        description="Mount name to change",
    )
    @discord.option("to_name", type=str, description="Mount name to assign")
    async def dbrenamemount(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
        from_name: str,
        to_name: str,
    ) -> None:
        """Edit the name of a mount in the database."""
        logger.info("/dbrenamemount invoked by %s", ctx.author.name)
        database = DataBase()
        from_item_id = await database.get_item_id(from_name)
        to_item_id = await database.get_item_id(to_name)
        no_from_name_found = len(from_item_id) == 0
        to_name_found = len(to_item_id) != 0
        if no_from_name_found:
            logger.warning(
                "Expansion %s mount %s not found, cancelling",
                expansion,
                from_name,
            )
            await ctx.send_response(
                content=f"I don't have a mount named `{from_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif to_name_found:
            logger.warning(
                "Expansion %s mount %s already in database, cancelling",
                expansion,
                to_name,
            )
            await ctx.send_response(
                content=f"I already have a mount named `{to_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info(
                "Renaming expansion %s mount %s to %s",
                expansion,
                from_name,
                to_name,
            )
            await database.edit_item_name(from_name, to_name)
            await ctx.send_response(
                content=f"Renamed `{expansion}` mount `{from_name}` to `{to_name}`.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/dbrenamemount OK")

    @discord.slash_command(
        name="dbrenameuser",
        description="(Admin only) Change the name of a user in the database",
    )
    @commands.has_role(547835267394830348)
    @discord.option("from_name", type=str, description="User name to change")
    @discord.option("to_name", type=str, description="User name to assign")
    async def dbrenameuser(
        self: Self,
        ctx: discord.ApplicationContext,
        from_name: str,
        to_name: str,
    ) -> None:
        """Change the name of a user in the database."""
        logger.info("/dbrenameuser invoked by %s", ctx.author.name)
        database = DataBase()
        from_name_exists = await database.check_user_exists(
            check_col="user_name",
            check_val=from_name,
        )
        to_name_exists = await database.check_user_exists(
            check_col="user_name",
            check_val=to_name,
        )
        if not from_name_exists:
            logger.warning("User %s not found, cancelling", from_name)
            await ctx.send_response(
                content=f"I don't have a user named `{from_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif to_name_exists:
            logger.warning("User %s already in database, cancelling", to_name)
            await ctx.send_response(
                content=f"I already have a user named `{to_name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Renaming user %s to %s", from_name, to_name)
            user_id = await database.get_user_id(from_name)
            query = "UPDATE users SET user_name = ? WHERE user_id = ?"
            params = (to_name, user_id)
            await database.db_execute_qmark(query, params)
            await ctx.send_response(
                content=f"User name `{from_name}` changed to `{to_name}`.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/dbrenameuser OK")

    @discord.slash_command(
        name="dbdeleteuser",
        description="(Admin only) Delete a user from the database.",
    )
    @commands.has_role(547835267394830348)
    @discord.option("name", type=str, description="Name of user to delete")
    async def dbdeleteuser(
        self: Self,
        ctx: discord.ApplicationContext,
        name: str,
    ) -> None:
        """Delete a user from the database."""
        logger.info("/dbdeleteuser invoked by %s", ctx.author.name)
        database = DataBase()
        from_name_exists = await database.check_user_exists(
            check_col="user_name",
            check_val=name,
        )
        if not from_name_exists:
            logger.warning("User %s not found, cancelling", name)
            await ctx.send_response(
                content=f"I don't have a user named `{name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            logger.info("Removing user %s from database", name)
            await database.delete_user()
            await ctx.send_response(
                content=f"User name `{name}` deleted.",
                ephemeral=True,
                delete_after=90,
            )
        logger.info("/dbdeleteuser OK")


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(DataOps(bot))
