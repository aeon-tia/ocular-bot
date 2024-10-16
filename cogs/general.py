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

    @discord.event
    async def on_ready(self: Self) -> None:
        """Create DB and print status message when bot comes online."""
        database = DataBase()
        await database.init_tables()

    @discord.slash_command(name="ocular", description="Confirm the bot is responsive")
    async def ocular(self: Self, ctx: discord.ApplicationContext) -> None:
        """Check if the bot responds."""
        await ctx.respond("We stand together!")

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
            await ctx.send_response(
                content="I already have a user with this name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        elif id_exists:
            await ctx.send_response(
                content="I already have a user with your discord ID in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
            await database.append_new_user(name=name, discord_id=ctx.author.id)
            await database.append_new_status(discord_id=ctx.author.id)
            await ctx.send_response(
                content=f"You have been added as `{name}` in my database.",
                ephemeral=True,
                delete_after=90,
            )

    @discord.slash_command(name="userlist", description="List users in the database")
    async def userlist(self: Self, ctx: discord.ApplicationContext) -> None:
        """Get the list of all users in the database."""
        database = DataBase()
        user_table = await database.read_table_polars("users")
        user_list = user_table.select("user_name").to_series().to_list()
        embed = discord.Embed(
            title="Users",
            description=f"List of users in the database: \n - {'\n - '.join(user_list)}",  # noqa: E501
            color=discord.Colour.blurple(),
        )
        await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)

    @discord.slash_command(name="mountlist", description="List available mount names")
    @discord.option(
        "expansion",
        type=str,
        autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
        description="Expansion to list mounts from",
    )
    async def mountlist(
        self: Self,
        ctx: discord.ApplicationContext,
        expansion: str,
    ) -> None:
        """Show a list of available mount names."""
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        embed = discord.Embed(
            title=f"{expansion.capitalize()} mounts",
            description=f"Available mount names are: \n - {'\n - '.join(item_names)}",
            color=discord.Colour.blurple(),
        )
        await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)

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
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        if name not in item_names:
            await ctx.send_response(
                content=f"`{name}` isn't a valid mount name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
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
        database = DataBase()
        item_names = await database.list_item_names(expansion)
        if name not in item_names:
            await ctx.send_response(
                content=f"`{name}` isn't a valid mount name in my database.",
                ephemeral=True,
                delete_after=90,
            )
        else:
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
        database = DataBase()
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

    @discord.slash_command(
        name="mostneeded",
        description="Count the most needed mounts",
    )
    @discord.option("n_out", type=int, description="Number of results to output")
    async def mostneeded(
        self: Self,
        ctx: discord.ApplicationContext,
        n_out: int,
    ) -> None:
        """Count which mounts are needed by the most users."""
        database = DataBase()
        meeded_mounts = await database.summarize_needed_mounts()
        output = meeded_mounts[0:n_out]
        embed = discord.Embed(
            title="Most commonly needed mounts",
            color=discord.Colour.blurple(),
        )
        embed.add_field(
            name=f"Top `{n_out}`",
            value=f" - {'\n - '.join(output)}",
            inline=True,
        )
        await ctx.send_response(embed=embed, ephemeral=True)


def setup(bot: discord.Bot) -> None:
    """Allow the bot to use this cog."""
    bot.add_cog(General(bot))
