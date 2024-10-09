"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import logging
import os

import discord
from discord.ext import commands
from dotenv import load_dotenv

from src.ocular.operations import DataBase

logging.basicConfig(
    filename="bot.log",
    encoding="utf-8",
    level=logging.INFO,
    style="{",
    format="{asctime} | {levelname:^8s} | {message}",
)

load_dotenv()
bot = discord.Bot()


async def get_mount_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    mounts = await database.list_item_names(
        table_name=ctx.options["kind"],
        expansion=ctx.options["expansion"],
    )
    return mounts  # noqa: RET504


async def get_expansion_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    expansions = await database.list_expansions(kind=ctx.options["kind"])
    return expansions  # noqa: RET504


@bot.event
async def on_ready() -> None:
    """Create DB and print status message when bot comes online."""
    database = DataBase()
    await database.init_tables()
    logging.info("%s is ready!", bot.user)


@bot.slash_command(name="ocular", description="Confirm the bot is responsive.")
async def ocular(ctx: discord.ApplicationContext) -> None:
    """Check if the bot responds."""
    logging.info("/ocular invoked by %s", ctx.author.name)
    await ctx.respond("We stand together!")
    logging.info("/ocular OK")


@bot.slash_command(
    name="dbcreatemount",
    description="(Admin only) Create a new mount in the database.",
)
@commands.is_owner()
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
async def dbcreatemount(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
    name: str,
) -> None:
    """Create new mounts in the database."""
    logging.info("/dbcreatemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_id = await database.get_item_ids(kind, name)
    already_exists = len(item_id) != 0
    if already_exists:
        logging.info("Stopping because matching mount name found")
        await ctx.send_response(
            content="I already have a mount with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info("Adding new %s %s to database", expansion, name)
        await database.add_new_item(kind, expansion, name)
        logging.info("Generating message")
        await ctx.send_response(
            content=f"Created `{expansion}` `{kind}` mount `{name}`",
            ephemeral=True,
            delete_after=90,
        )
    logging.info("/dbcreatemount OK")


@bot.slash_command(
    name="dbdeletemount",
    description="(Admin only) Delete a mount from the database.",
)
@commands.is_owner()
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
async def dbdeletemount(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
    name: str,
) -> None:
    """Delete a mount from the database."""
    logging.info("/dbdeletemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_id = await database.get_item_ids(kind, name)
    no_match = len(item_id) == 0
    if no_match:
        logging.info("Stopping because no matching mount name found")
        await ctx.send_response(
            content="I don't have a mount with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info("Deleting %s %s mount %s", expansion, kind, name)
        await database.delete_item(kind, name)
        logging.info("Generating message")
        await ctx.send_response(
            content=f"Deleted `{expansion}` `{kind}` mount `{name}` from the database.",
            ephemeral=True,
            delete_after=90,
        )
    logging.info("/dbdeletemount OK")


@bot.slash_command(
    name="dbrenamemount",
    description="(Admin only) Rename a mount in the database.",
)
@commands.is_owner()
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "from_name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
@discord.option("to_name", type=str)
async def dbrenamemount(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
    from_name: str,
    to_name: str,
) -> None:
    """Edit the name of a mount in the database."""
    logging.info("/dbrenamemount invoked by %s", ctx.author.name)
    database = DataBase()
    from_item_id = await database.get_item_ids(kind, from_name)
    to_item_id = await database.get_item_ids(kind, to_name)
    no_from_name_found = len(from_item_id) == 0
    to_name_found = len(to_item_id) != 0
    if no_from_name_found:
        logging.info("Stopping because mount name %s not found", from_name)
        await ctx.send_response(
            content=f"I don't have a mount named `{from_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    elif to_name_found:
        logging.info("Stopping because mount name %s found", to_name)
        await ctx.send_response(
            content=f"I already have a mount named `{to_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info(
            "Renaming %s %s mount from %s to %s",
            expansion,
            kind,
            from_name,
            to_name,
        )
        await database.edit_item_name(kind, from_name, to_name)
        logging.info("Generating message")
        await ctx.send_response(
            content=f"Renamed `{expansion}` `{kind}` `{from_name}` to {to_name}.",
            ephemeral=True,
            delete_after=90,
        )
    logging.info("/dbrenamemount OK")


@bot.slash_command(name="addme", description="Add yourself to the bot's user list.")
@discord.option("name", type=str)
async def addme(ctx: discord.ApplicationContext, name: str) -> None:
    """Add user to the user table."""
    logging.info("/addme invoked by %s", ctx.author.name)
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
        logging.info("Username %s not added because already exists", name)
        await ctx.send_response(
            content="I already have a user with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    elif id_exists:
        logging.info("Discord ID %s not added because already exists", ctx.author.id)
        await ctx.send_response(
            content="I already have a user with your discord ID in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info("Creating new user")
        await database.append_new_user(name=name, discord_id=ctx.author.id)
        await database.append_new_status(discord_id=ctx.author.id)
        await ctx.send_response(
            content=f"You have been added as {name} in the database.",
            ephemeral=True,
            delete_after=90,
        )
        logging.info(
            "User entry created for %s with discord ID %s",
            name,
            ctx.author.id,
        )
    logging.info("/addme OK")


@bot.slash_command(name="mountlist", description="List available mount names.")
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
async def mountlist(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
) -> None:
    """Show a list of available mount names."""
    logging.info("/mountlist invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(
        table_name=kind,
        expansion_name=expansion,
    )
    logging.info("Item lists generated")
    embed = discord.Embed(
        title=f"{expansion.capitalize()} {kind} mounts",
        description=f"Available mount names are: \n - {'\n - '.join(item_names)}",
        color=discord.Colour.blurple(),
    )
    logging.info("Message generated")
    await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)
    logging.info("/mountlist OK")


@bot.slash_command(name="addmount", description="Add mounts to your list.")
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
async def addmount(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
    name: str,
) -> None:
    """Add items to a user in the status table."""
    logging.info("/addmount invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(kind)
    items_dne = list(set(name.split(",")) - set(item_names))
    if len(items_dne) != 0:
        logging.info("Items not added, invalid names provided")
        await ctx.send_response(
            content=f"`{", ".join(items_dne)}` are not valid mount names in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info("Adding items %s under user %s", name, ctx.author.name)
        await database.update_user_items(
            action="add",
            user=ctx.author.id,
            item_kind=kind,
            item_names=name,
        )
        logging.info("Items added")
        await ctx.send_response(
            content=f"Added {expansion} mounts: {name}",
            ephemeral=True,
            delete_after=90,
        )
    logging.info("/addmount OK")


@bot.slash_command(name="removemount", description="Remove mounts from your list.")
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
async def removemount(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
    name: str,
) -> None:
    """Add items to a user in the status table."""
    logging.info("/removemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(kind)
    items_dne = list(set(name.split(",")) - set(item_names))
    if len(items_dne) != 0:
        await ctx.send_response(
            content=f"`{", ".join(items_dne)}` are not valid mount names in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        logging.info("Removing items %s from user %s", name, ctx.author.name)
        await database.update_user_items(
            user=ctx.author.id,
            action="remove",
            item_kind=kind,
            item_names=name,
        )
        logging.info("Items removed")
        await ctx.send_response(
            content=f"Removed {expansion} mounts: {name}",
            ephemeral=True,
            delete_after=90,
        )
    logging.info("/removemount OK")


@bot.slash_command(name="mymounts", description="View your mounts.")
@discord.option("kind", type=str, choices=["trials", "raids"])
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
async def mymounts(
    ctx: discord.ApplicationContext,
    kind: str,
    expansion: str,
) -> None:
    """View list of held and needed mounts."""
    logging.info("/mymounts invoked by %s", ctx.author.name)
    database = DataBase()
    has_mounts = await database.list_user_items(
        user=ctx.author.id,
        check_type="has",
        kind=kind,
        expansion=expansion,
    )
    needs_mounts = await database.list_user_items(
        user=ctx.author.id,
        check_type="needs",
        kind=kind,
        expansion=expansion,
    )
    logging.info("Item lists generated")
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
        title=f"{expansion.capitalize()} {kind}",
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
    logging.info("Message generated")
    await ctx.respond(embed=embed)
    logging.info("/mymounts OK")


def main() -> None:
    """Run program."""
    logging.info("Launching Ocular")
    bot.run(os.getenv("TOKEN"))


if __name__ == "__main__":
    main()
