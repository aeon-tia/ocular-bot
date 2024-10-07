"""Ocular bot - Discord bot for tracking FFXIV mount progress."""

import os

import discord
from dotenv import load_dotenv

from src.ocular.operations import DataBase

load_dotenv()
bot = discord.Bot()


async def get_mount_names(ctx: discord.AutocompleteContext) -> list[str]:
    """Fetch list of mount names for autocomplete."""
    database = DataBase()
    mounts = await database.list_item_names(
        table_name=ctx.options["kind"],
        expansion_name=ctx.options["expansion"],
    )
    return mounts  # noqa: RET504


@bot.event
async def on_ready() -> None:
    """Create DB and print status message when bot comes online."""
    database = DataBase()
    await database.init_tables()
    print(f"{bot.user} is ready!")  # noqa: T201


@bot.slash_command(name="oping", description="Confirm the bot is responsive.")
async def oping(ctx: discord.ApplicationContext) -> None:
    """Check if the bot responds."""
    await ctx.respond("I'm online!")


@bot.slash_command(name="oiam", description="Add yourself to the bot's user list.")
async def oiam(ctx: discord.ApplicationContext, name: discord.Option(str)) -> None:
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
            content=f"You have been added as {name} in the database.",
            ephemeral=True,
            delete_after=90,
        )


@bot.slash_command(name="omounts", description="List available mount names.")
async def omounts(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    expansion: discord.Option(
        str,
        choices=[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalkers",
            "dawntrail",
        ],
    ),
) -> None:
    """Show a list of available mount names."""
    database = DataBase()
    item_names = await database.list_item_names(
        table_name=kind,
        expansion_name=expansion,
    )
    embed = discord.Embed(
        title=f"{expansion.capitalize()} {kind} mounts",
        description=f"Available mount names are: \n - {'\n - '.join(item_names)}",
        color=discord.Colour.blurple(),
    )
    await ctx.send_response(embed=embed, ephemeral=True, delete_after=90)


@bot.slash_command(name="oadd", description="Add mounts to your list.")
async def oadd(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    expansion: discord.Option(
        str,
        choices=[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalker",
            "dawntrail",
        ],
    ),
    names: discord.Option(
        str, autocomplete=discord.utils.basic_autocomplete(get_mount_names)
    ),
) -> None:
    """Add items to a user in the status table."""
    database = DataBase()
    item_names = await database.list_item_names(kind)
    items_dne = list(set(names.split(",")) - set(item_names))
    if len(items_dne) != 0:
        await ctx.send_response(
            content=f"`{", ".join(items_dne)}` are not valid mount names in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        await database.update_user_items(
            action="add",
            user=ctx.author.id,
            item_kind=kind,
            item_names=names,
        )
        await ctx.send_response(
            content=f"Added {expansion} mounts: {names}",
            ephemeral=True,
            delete_after=90,
        )


@bot.slash_command(name="oremove", description="Remove mounts from your list.")
async def oremove(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    expansion: discord.Option(
        str,
        choices=[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalker",
            "dawntrail",
        ],
    ),
    names: discord.Option(
        str, autocomplete=discord.utils.basic_autocomplete(get_mount_names)
    ),
) -> None:
    """Add items to a user in the status table."""
    database = DataBase()
    item_names = await database.list_item_names(kind)
    items_dne = list(set(names.split(",")) - set(item_names))
    if len(items_dne) != 0:
        await ctx.send_response(
            content=f"`{", ".join(items_dne)}` are not valid mount names in my database.",
            ephemeral=True,
            delete_after=90,
        )
    else:
        await database.update_user_items(
            user=ctx.author.id,
            action="remove",
            item_kind=kind,
            item_names=names,
        )
        await ctx.send_response(
            content=f"Removed {expansion} mounts: {names}",
            ephemeral=True,
            delete_after=90,
        )


@bot.slash_command(name="oview", description="View your mounts.")
async def oview(
    ctx: discord.ApplicationContext,
    kind: discord.Option(str, choices=["trials", "raids"]),
    expansion: discord.Option(
        str,
        choices=[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalker",
            "dawntrail",
        ],
    ),
) -> None:
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
    await ctx.respond(embed=embed)


def main() -> None:
    """Run program."""
    bot.run(os.getenv("TOKEN"))


if __name__ == "__main__":
    main()
