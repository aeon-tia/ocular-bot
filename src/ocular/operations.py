"""Database operations for discord bot."""

import json
from pathlib import Path
from typing import Literal, Self

import aiosqlite
import polars as pl
import uuid6


def dict_factory(cursor: aiosqlite.Cursor, row: aiosqlite.Row) -> dict:
    """Convert rows returned by a cursor operation to dicts."""
    fields = [column[0] for column in cursor.description]
    return dict(zip(fields, row, strict=True))


class DataBase:
    """Class storing methods for database operations."""

    def __init__(self) -> None:
        """Methods for database operations."""
        self.db_path = "./data/bot.db"

    async def db_execute_literal(self: Self, query: str) -> None:
        """Execute a DB write query directly string."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.execute(query)
            await db.commit()

    async def db_execute_qmark(self: Self, query: str, params: tuple) -> None:
        """Execute a DB query with the qmarks placeholder syntax."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.execute(query, params)
            await db.commit()

    async def db_execute_dictuple(self: Self, query: str, rows: tuple[dict]) -> None:
        """Execute a DB query with the tuple of dict placeholder syntax."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.executemany(query, rows)
            await db.commit()

    async def db_read_table(self: Self, query: str) -> tuple[dict]:
        """Read a DB table and return all rows."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.execute(query)
            return await cs.fetchall()

    async def init_mount_table(self: Self) -> tuple[dict]:
        """Create rows for initializing mounts table."""
        trials = tuple(json.load(Path.open("./assets/inputs/mounts.json"))["trials"])
        raids = tuple(json.load(Path.open("./assets/inputs/mounts.json"))["raids"])
        query = """
            CREATE TABLE IF NOT EXISTS
            mounts(item_id STRING, item_name STRING, item_expac STRING)
        """
        await self.db_execute_literal(query)
        table_shape = await self.check_table_shape("mounts")
        if table_shape != (0, 0):
            msg = "Mounts table is not empty, initial rows not appended."
            raise AssertionError(msg)
        await self.append_to_mount_table(trials)
        await self.append_to_mount_table(raids)

    async def init_user_table(self: Self) -> None:
        """Initialize user table."""
        query = """
            CREATE TABLE IF NOT EXISTS
            users(user_id STRING, user_name STRING, user_discord_id INTEGER)
        """
        await self.db_execute_literal(query)

    async def init_status_table(self: Self) -> None:
        """Initialize user table."""
        query = """
            CREATE TABLE IF NOT EXISTS
            status(
                user_id STRING,
                item_id STRING,
                has_item INTEGER
            )
        """
        await self.db_execute_literal(query)

    async def init_tables(self: Self) -> None:
        """Initialize database tables."""
        data_dir = Path("./data")
        if not data_dir.joinpath("bot.db").exists():
            data_dir.mkdir(parents=True, exist_ok=True)
            await self.init_user_table()
            await self.init_mount_table()
            await self.init_status_table()

    def create_user_row(self: Self, name: str, discord_id: int) -> tuple[dict]:
        """Create a new user ID as a row for the user table."""
        new_id = uuid6.uuid7().hex
        return ({"user_name": name, "user_id": new_id, "user_discord_id": discord_id},)

    def create_item_row(self: Self, name: str, expansion: str) -> tuple[dict]:
        """Create a new item ID as a row for an item table."""
        new_id = uuid6.uuid7().hex
        return ({"item_id": new_id, "item_name": name, "item_expac": expansion},)

    async def append_new_user(self: Self, name: str, discord_id: int) -> None:
        """Create user table rows from a string of comma-separated user names."""
        row = self.create_user_row(name, discord_id)
        query = "INSERT INTO users VALUES(:user_id, :user_name, :user_discord_id)"
        await self.db_execute_dictuple(query, row)

    async def append_to_mount_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new mounts to the mount table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to mount table. Must be keyed by item_id and
            item_name. Each new row should be a separate dict.

        """
        query = "INSERT INTO mounts VALUES(:item_id, :item_name, :item_expac)"
        await self.db_execute_dictuple(query, new_rows)

    async def append_to_status_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new status rows to the status table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to status table. Must be keyed by user_id,
            item_id, and has_item. Each new row should be a
            separate dict.

        """
        query = "INSERT INTO status VALUES(:user_id, :item_id, :has_item)"
        await self.db_execute_dictuple(query, new_rows)

    async def get_user_id(self: Self, user_name: str) -> str:
        """Get a user id from the user table.

        Parameters
        ----------
        user_name : str
            Name of user to get database ID for.

        """
        usr = tuple(x for x in [user_name])
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.execute("SELECT user_id FROM users WHERE user_name = ?", usr)
            user_row = await cs.fetchone()
            return user_row["user_id"]

    async def get_user_discord_id(self: Self, user_name: str) -> int:
        """Get a user discord ID from user name."""
        user_table = await self.read_table_polars("users")
        return (
            user_table.filter(pl.col("user_name") == user_name)
            .select("user_discord_id")
            .to_series()
            .to_list()
        )

    async def get_user_from_discord_id(self: Self, discord_id: str) -> str:
        """Get a user id from the user table.

        Parameters
        ----------
        discord_id : str
            Discord ID of user to get database ID for.

        """
        usr = tuple(x for x in [discord_id])
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = dict_factory
            cs = await db.cursor()
            await cs.execute("SELECT user_id FROM users WHERE user_discord_id = ?", usr)
            user_row = await cs.fetchone()
            return user_row["user_id"]

    async def get_user_table(self: Self) -> tuple[dict]:
        """Get user table as tuple of dict."""
        query = "SELECT * FROM users"
        return await self.db_read_table(query)

    async def get_mount_table(self: Self) -> tuple[dict]:
        """Get mount table as tuple of dict."""
        query = "SELECT * FROM mounts"
        return await self.db_read_table(query)

    async def get_status_table(self: Self) -> tuple[dict]:
        """Get raid table as tuple of dict."""
        query = "SELECT * FROM status"
        return await self.db_read_table(query)

    async def read_table_polars(
        self: Self,
        table_name: Literal["users", "mounts", "status"],
    ) -> pl.DataFrame:
        """Read a database table into a Polars DataFrame.

        Parameters
        ----------
        table_name : Literal["users", "mounts", "status"]
            Name of database table to return.

        Returns
        -------
        table : pl.DataFrame
            Polars dataframe of the requested database table.

        """
        if table_name == "users":
            table = pl.DataFrame(await self.get_user_table())
        if table_name == "mounts":
            table = pl.DataFrame(await self.get_mount_table())
        if table_name == "status":
            table = pl.DataFrame(await self.get_status_table())
        return table

    async def append_new_status(self: Self, discord_id: str) -> tuple[dict]:
        """Create status table rows for a new user."""
        user = await self.get_user_from_discord_id(discord_id)
        mounts = await self.read_table_polars("mounts")
        rows = tuple(
            mounts
            .with_columns(
                user_id=pl.lit(user),
                has_item=pl.lit(0).cast(pl.Int64),
            )
            .to_dicts(),
        )
        await self.append_to_status_table(rows)
        return rows

    async def get_item_ids(
        self: Self,
        item_names: str,
    ) -> tuple[int]:
        """Get a list of item IDs from a specified item table."""
        item_names = item_names.split(",")
        items = await self.read_table_polars("mounts")
        items = items.filter(pl.col("item_name").is_in(item_names)).select("item_id")
        return tuple(items.to_series().to_list())

    async def update_user_items(
        self: Self,
        action: Literal["add", "remove"],
        user: int,
        item_names: list[str],
    ) -> tuple[dict]:
        """Update entries for a user in the status table."""
        user_id = await self.get_user_from_discord_id(user)
        items = await self.get_item_ids(item_names)
        if action == "add":
            has_item_entry = 1
        elif action == "remove":
            has_item_entry = 0
        else:
            msg = "action must be one of ['add', 'remove']"
            raise ValueError(msg)
        query = """
            UPDATE status
            SET has_item = ?
            WHERE user_id = ? AND item_id = ?
        """
        for item in items:
            params = (has_item_entry, user_id, item)
            await self.db_execute_qmark(query, params)

    async def check_table_shape(
        self: Self,
        table_name: Literal["users", "mounts", "status"],
    ) -> bool:
        """Check the shape of a database table."""
        table = await self.read_table_polars(table_name)
        return table.shape

    async def check_user_exists(
        self: Self,
        check_col: Literal["user_name", "user_id", "user_discord_id"],
        check_val: str | int,
    ) -> bool:
        """Check if a user already exists in the users table."""
        table = await self.read_table_polars("users")
        if table.shape[0] == 0:
            return False
        return not (
            table.filter(pl.col(check_col) == check_val).select("user_id").is_empty()
        )

    async def list_item_names(
        self: Self,
        expansion: None | str = None,
    ) -> list[str]:
        """Get a list of item names from a DB table."""
        table = await self.read_table_polars("mounts")
        if expansion is None:
            return table.select("item_name").to_series().to_list()
        return (
            table.filter(pl.col("item_expac") == expansion)
            .select("item_name")
            .to_series()
            .to_list()
        )

    async def list_expansions(
        self: Self,
    ) -> list[str]:
        """Get list of expansions in a table."""
        table = await self.read_table_polars("mounts")
        return table.select("item_expac").to_series().unique().to_list()

    async def list_user_items(
        self: Self,
        user: int,
        check_type: Literal["has", "needs"],
        expansion: str,
    ) -> list[str]:
        """Get list of mounts a user has or needs."""
        user_id = await self.get_user_from_discord_id(user)
        status_table = await self.read_table_polars("status")
        item_table = await self.read_table_polars("mounts")
        check_val = 1 if check_type == "has" else 0
        status_table = (
            status_table.filter(
                (pl.col("user_id") == user_id)
                & (pl.col("has_item") == check_val),
            )
        ).select("item_id")
        item_names = (
            status_table.join(item_table, on="item_id", how="inner")
            .filter(pl.col("item_expac") == expansion)
            .select("item_name")
            .to_series()
            .to_list()
        )
        if len(item_names) == 0:
            return ["none"]
        return item_names

    async def edit_item_name(
        self: Self,
        old_name: str,
        new_name: str,
    ) -> None:
        """Edit an item name."""
        item_id = await self.get_item_ids(old_name)
        query = "UPDATE mounts SET item_name = ? WHERE item_id = ?"
        params = (new_name, item_id[0])
        await self.db_execute_qmark(query, params)

    async def add_new_item(
        self: Self,
        expansion: str,
        name: str,
    ) -> None:
        """Edit an item name."""
        new_row = self.create_item_row(name, expansion)
        await self.append_to_mount_table(new_row)

    async def delete_item(
        self: Self,
        name: str,
    ) -> None:
        """Remove mounts from the database."""
        item_id = await self.get_item_ids(name)
        params = tuple(item_id)
        query = "DELETE FROM mounts WHERE item_id = ?"
        await self.db_execute_qmark(query, params)

    async def delete_user(self: Self, name: str) -> None:
        """Remove users from the database."""
        user_id = await self.get_user_id(name)
        params = (user_id,)
        query = "DELETE FROM users WHERE user_id = ?"
        await self.db_execute_qmark(query, params)
