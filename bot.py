"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import logging
import os
from logging.handlers import RotatingFileHandler

import discord
from discord.ext import commands
from dotenv import load_dotenv

from src.ocular.operations import DataBase

log_formatter = logging.Formatter(
    "{asctime} | {levelname:8s} | {funcName}:{lineno} {message}", style="{",
)
log_handler = RotatingFileHandler(
    filename="bot.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=2,
)
log_handler.setFormatter(log_formatter)
bot_log = logging.getLogger()
bot_log.setLevel(logging.INFO)
bot_log.addHandler(log_handler)

load_dotenv()
bot = discord.Bot()


async def get_mount_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    mounts = await database.list_item_names(
        expansion=ctx.options["expansion"],
    )
    return mounts  # noqa: RET504


async def get_expansion_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    expansions = await database.list_expansions()
    return expansions  # noqa: RET504


@bot.event
async def on_ready() -> None:
    """Create DB and print status message when bot comes online."""
    database = DataBase()
    await database.init_tables()
    bot_log.info("%s is ready!", bot.user)


@bot.slash_command(name="ocular", description="Confirm the bot is responsive.")
async def ocular(ctx: discord.ApplicationContext) -> None:
    """Check if the bot responds."""
    bot_log.info("/ocular invoked by %s", ctx.author.name)
    await ctx.respond("We stand together!")
    bot_log.info("/ocular OK")


@bot.slash_command(
    name="dbcreatemount",
    description="(Admin only) Create a new mount in the database.",
)
@commands.has_role(547835267394830348)
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
    expansion: str,
    name: str,
) -> None:
    """Create new mounts in the database."""
    bot_log.info("/dbcreatemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_id = await database.get_item_ids(name)
    already_exists = len(item_id) != 0
    if already_exists:
        bot_log.info("Stopping because matching mount name found")
        await ctx.send_response(
            content="I already have a mount with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Adding new %s %s to database", expansion, name)
        await database.add_new_item(expansion, name)
        bot_log.info("Generating message")
        await ctx.send_response(
            content=f"Created `{expansion}` mount `{name}`",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/dbcreatemount OK")


@bot.slash_command(
    name="dbdeletemount",
    description="(Admin only) Delete a mount from the database.",
)
@commands.has_role(547835267394830348)
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
    expansion: str,
    name: str,
) -> None:
    """Delete a mount from the database."""
    bot_log.info("/dbdeletemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_id = await database.get_item_ids(name)
    no_match = len(item_id) == 0
    if no_match:
        bot_log.info("Stopping because no matching mount name found")
        await ctx.send_response(
            content="I don't have a mount with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Deleting %s %s mount %s", expansion, name)
        await database.delete_item(name)
        bot_log.info("Generating message")
        await ctx.send_response(
            content=f"Deleted `{expansion}` mount `{name}` from the database.",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/dbdeletemount OK")


@bot.slash_command(
    name="dbrenamemount",
    description="(Admin only) Rename a mount in the database.",
)
@commands.has_role(547835267394830348)
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
    expansion: str,
    from_name: str,
    to_name: str,
) -> None:
    """Edit the name of a mount in the database."""
    bot_log.info("/dbrenamemount invoked by %s", ctx.author.name)
    database = DataBase()
    from_item_id = await database.get_item_ids(from_name)
    to_item_id = await database.get_item_ids(to_name)
    no_from_name_found = len(from_item_id) == 0
    to_name_found = len(to_item_id) != 0
    if no_from_name_found:
        bot_log.info("Stopping because mount name %s not found", from_name)
        await ctx.send_response(
            content=f"I don't have a mount named `{from_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    elif to_name_found:
        bot_log.info("Stopping because mount name %s found", to_name)
        await ctx.send_response(
            content=f"I already have a mount named `{to_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info(
            "Renaming %s %s mount from %s to %s", expansion, from_name, to_name,
        )
        await database.edit_item_name("mounts", from_name, to_name)
        bot_log.info("Generating message")
        await ctx.send_response(
            content=f"Renamed `{expansion}` mount `{from_name}` to `{to_name}`.",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/dbrenamemount OK")


@bot.slash_command(
    name="dbrenameuser",
    description="(Admin only) Change the name of a user in the database.",
)
@commands.has_role(547835267394830348)
async def dbrenameuser(
    ctx: discord.ApplicationContext,
    from_name: str,
    to_name: str,
) -> None:
    """Change the name of a user in the database."""
    bot_log.info("/dbrenameuser invoked by %s", ctx.author.name)
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
        bot_log.info("Username not changed because user %s not found", from_name)
        await ctx.send_response(
            content=f"I don't have a user named `{from_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    elif to_name_exists:
        bot_log.info("Username not changed because user %s already exists", to_name)
        await ctx.send_response(
            content=f"I already have a user named `{to_name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Changing user name %s to %s", from_name, to_name)
        user_id = await database.get_user_id(from_name)
        query = "UPDATE users SET user_name = ? WHERE user_id = ?"
        params = (to_name, user_id)
        await database.db_execute_qmark(query, params)
        bot_log.info("Generating message")
        await ctx.send_response(
            content=f"User name `{from_name}` changed to `{to_name}`.",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/dbrenameuser OK")


@bot.slash_command(name="addme", description="Add yourself to the bot's user list.")
@discord.option("name", type=str)
async def addme(ctx: discord.ApplicationContext, name: str) -> None:
    """Add user to the user table."""
    bot_log.info("/addme invoked by %s", ctx.author.name)
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
        bot_log.info("Username %s not added because already exists", name)
        await ctx.send_response(
            content="I already have a user with this name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    elif id_exists:
        bot_log.info("Discord ID %s not added because already exists", ctx.author.id)
        await ctx.send_response(
            content="I already have a user with your discord ID in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Creating new user")
        await database.append_new_user(name=name, discord_id=ctx.author.id)
        await database.append_new_status(discord_id=ctx.author.id)
        await ctx.send_response(
            content=f"You have been added as `{name}` in my database.",
            ephemeral=True,
            delete_after=90,
        )
        bot_log.info(
            "User entry created for %s with discord ID %s",
            name,
            ctx.author.id,
        )
    bot_log.info("/addme OK")


@bot.slash_command(name="userlist", description="List users in the database.")
async def userlist(ctx: discord.ApplicationContext) -> None:
    """Get the list of all users in the database."""
    bot_log.info("/userlist invoked by %s", ctx.author.name)
    database = DataBase()
    user_table = await database.read_table_polars("users")
    user_list = user_table.select("user_name").to_series().to_list()
    bot_log.info("Item lists generated")
    embed = discord.Embed(
        title="Users",
        description=f"List of users in the database: \n - {'\n - '.join(user_list)}",
        color=discord.Colour.blurple(),
    )
    bot_log.info("Message generated")
    await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)
    bot_log.info("/userlist OK")


@bot.slash_command(name="mountlist", description="List available mount names.")
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
async def mountlist(
    ctx: discord.ApplicationContext,
    expansion: str,
) -> None:
    """Show a list of available mount names."""
    bot_log.info("/mountlist invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(expansion)
    bot_log.info("Item lists generated")
    embed = discord.Embed(
        title=f"{expansion.capitalize()} mounts",
        description=f"Available mount names are: \n - {'\n - '.join(item_names)}",
        color=discord.Colour.blurple(),
    )
    bot_log.info("Message generated")
    await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)
    bot_log.info("/mountlist OK")


@bot.slash_command(name="addmount", description="Add mounts to your list.")
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
    expansion: str,
    name: str,
) -> None:
    """Add items to a user in the status table."""
    bot_log.info("/addmount invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(expansion)
    if name not in item_names:
        bot_log.info("Items not added, invalid names provided")
        await ctx.send_response(
            content=f"`{name}` isn't a valid mount name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Adding items %s under user %s", name, ctx.author.name)
        await database.update_user_items(
            action="add",
            user=ctx.author.id,
            item_names=name,
        )
        bot_log.info("Items added")
        await ctx.send_response(
            content=f"Added `{expansion}` mount `{name}`",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/addmount OK")


@bot.slash_command(
    name="adminaddmount",
    description="(Admin only) Add mounts for a user.",
)
@commands.has_role(547835267394830348)
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "mount_name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
@discord.option("user_name", type=str)
async def adminaddmount(
    ctx: discord.ApplicationContext,
    expansion: str,
    mount_name: str,
    user_name: str,
) -> None:
    """Add items to a user other than the author in the status table."""
    bot_log.info("/adminaddmount invoked by %s", ctx.author.name)
    database = DataBase()
    user_did = await database.get_user_discord_id(user_name)
    item_names = await database.list_item_names(expansion)
    if len(user_did) == 0:
        bot_log.info("Items not added, invalid user name provided")
        await ctx.send_response(
            content=f"`{user_name}` isn't a valid user name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    if mount_name not in item_names:
        bot_log.info("Items not added, invalid mount name provided")
        await ctx.send_response(
            content=f"`{mount_name}` isn't a valid mount name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Adding items %s for user %s", mount_name, user_name)
        await database.update_user_items(
            action="add",
            user=user_did[0],
            item_names=mount_name,
        )
        bot_log.info("Items added")
        await ctx.send_response(
            content=f"Added `{expansion}` mount `{mount_name}` for `{user_name}`",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/adminaddmount OK")


@bot.slash_command(name="removemount", description="Remove mounts from your list.")
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
    expansion: str,
    name: str,
) -> None:
    """Add items to a user in the status table."""
    bot_log.info("/removemount invoked by %s", ctx.author.name)
    database = DataBase()
    item_names = await database.list_item_names(expansion)
    if name not in item_names:
        bot_log.info("Items not removed, invalid names provided")
        await ctx.send_response(
            content=f"`{name}` isn't a valid mount name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Removing items %s from user %s", name, ctx.author.name)
        await database.update_user_items(
            user=ctx.author.id,
            action="remove",
            item_names=name,
        )
        bot_log.info("Items removed")
        await ctx.send_response(
            content=f"Removed `{expansion}` mount `{name}`",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/removemount OK")


@bot.slash_command(
    name="adminremovemount",
    description="(Admin only) Remove mounts from a user.",
)
@commands.has_role(547835267394830348)
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
@discord.option(
    "mount_name",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_mount_names),
)
@discord.option("user_name", type=str)
async def adminremovemount(
    ctx: discord.ApplicationContext,
    expansion: str,
    mount_name: str,
    user_name: str,
) -> None:
    """Add items to a user other than the author in the status table."""
    bot_log.info("/adminremovemount invoked by %s", ctx.author.name)
    database = DataBase()
    user_did = await database.get_user_discord_id(user_name)
    item_names = await database.list_item_names(expansion)
    if len(user_did) == 0:
        bot_log.info("Items not removed, invalid user name provided")
        await ctx.send_response(
            content=f"`{user_name}` isn't a valid user name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    if mount_name not in item_names:
        bot_log.info("Items not removed, invalid mount name provided")
        await ctx.send_response(
            content=f"`{mount_name}` isn't a valid mount name in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        bot_log.info("Removing items %s from user %s", mount_name, user_name)
        await database.update_user_items(
            action="remove",
            user=user_did[0],
            item_names=mount_name,
        )
        bot_log.info("Items removed")
        await ctx.send_response(
            content=f"Removed `{expansion}` mount `{mount_name}` from `{user_name}`",
            ephemeral=True,
            delete_after=90,
        )
    bot_log.info("/adminremovemount OK")


@bot.slash_command(name="mymounts", description="View your mounts.")
@discord.option(
    "expansion",
    type=str,
    autocomplete=discord.utils.basic_autocomplete(get_expansion_names),
)
async def mymounts(
    ctx: discord.ApplicationContext,
    expansion: str,
) -> None:
    """View list of held and needed mounts."""
    bot_log.info("/mymounts invoked by %s", ctx.author.name)
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
    bot_log.info("Item lists generated")
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
    bot_log.info("Message generated")
    await ctx.respond(embed=embed)
    bot_log.info("/mymounts OK")


def main() -> None:
    """Run program."""
    bot_log.info("Launching Ocular")
    bot.run(os.getenv("TOKEN"))


if __name__ == "__main__":
    main()
