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

    async def init_trial_table(self: Self) -> tuple[dict]:
        """Create rows for initializing trials table."""
        rows = tuple(json.load(Path.open("./assets/inputs/mounts.json"))["trials"])
        query = """
            CREATE TABLE IF NOT EXISTS
            trials(item_id STRING, item_name STRING, item_expac STRING)
        """
        await self.db_execute_literal(query)
        table_shape = await self.check_table_shape("trials")
        if table_shape != (0, 0):
            msg = "Trials table is not empty, initial rows not appended."
            raise AssertionError(msg)
        await self.append_to_trial_table(rows)

    async def init_raid_table(self: Self) -> tuple[dict]:
        """Create rows for initializing raid table."""
        rows = tuple(json.load(Path.open("./assets/inputs/mounts.json"))["raids"])
        query = """
            CREATE TABLE IF NOT EXISTS
            raids(item_id STRING, item_name STRING, item_expac STRING)
        """
        await self.db_execute_literal(query)
        table_shape = await self.check_table_shape("raids")
        if table_shape != (0, 0):
            msg = "Raids table is not empty, initial rows not appended."
            raise AssertionError(msg)
        await self.append_to_raid_table(rows)

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
                item_kind STRING,
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
            await self.init_raid_table()
            await self.init_trial_table()
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

    async def append_to_trial_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new trials to the trials table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to mount table. Must be keyed by item_id and
            item_name. Each new row should be a separate dict.

        """
        query = "INSERT INTO trials VALUES(:item_id, :item_name, :item_expac)"
        await self.db_execute_dictuple(query, new_rows)

    async def append_to_raid_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new raids to the raids table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to raid table. Must be keyed by item_id and
            item_name. Each new row should be a separate dict.

        """
        query = "INSERT INTO raids VALUES(:item_id, :item_name, :item_expac)"
        await self.db_execute_dictuple(query, new_rows)

    async def append_to_status_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new status rows to the status table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to status table. Must be keyed by user_id,
            item_id, item_kind and has_item. Each new row should be a
            separate dict.

        """
        query = "INSERT INTO status VALUES(:user_id, :item_id, :item_kind, :has_item)"
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

    async def get_trial_table(self: Self) -> tuple[dict]:
        """Get trial table as tuple of dict."""
        query = "SELECT * FROM trials"
        return await self.db_read_table(query)

    async def get_raid_table(self: Self) -> tuple[dict]:
        """Get raid table as tuple of dict."""
        query = "SELECT * FROM raids"
        return await self.db_read_table(query)

    async def get_status_table(self: Self) -> tuple[dict]:
        """Get raid table as tuple of dict."""
        query = "SELECT * FROM status"
        return await self.db_read_table(query)

    async def read_table_polars(
        self: Self,
        table_name: Literal["users", "trials", "raids", "status"],
    ) -> pl.DataFrame:
        """Read a database table into a Polars DataFrame.

        Parameters
        ----------
        table_name : Literal["users", "trials", "raids", "status"]
            Name of database table to return.

        Returns
        -------
        table : pl.DataFrame
            Polars dataframe of the requested database table.

        """
        if table_name == "users":
            table = pl.DataFrame(await self.get_user_table())
        if table_name == "trials":
            table = pl.DataFrame(await self.get_trial_table())
        if table_name == "raids":
            table = pl.DataFrame(await self.get_raid_table())
        if table_name == "status":
            table = pl.DataFrame(await self.get_status_table())
        return table

    async def append_new_status(self: Self, discord_id: str) -> tuple[dict]:
        """Create status table rows for a new user."""
        user = await self.get_user_from_discord_id(discord_id)
        trials = await self.read_table_polars("trials")
        raids = await self.read_table_polars("raids")
        trials = trials.select(["item_id"]).with_columns(item_kind=pl.lit("trials"))
        raids = raids.select(["item_id"]).with_columns(item_kind=pl.lit("raids"))
        rows = tuple(
            pl.concat([trials, raids])
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
        item_kind: Literal["trials", "raids"],
        item_names: str,
    ) -> tuple[int]:
        """Get a list of item IDs from a specified item table."""
        item_names = item_names.split(",")
        items = await self.read_table_polars(item_kind)
        items = items.filter(pl.col("item_name").is_in(item_names)).select("item_id")
        return tuple(items.to_series().to_list())

    async def update_user_items(
        self: Self,
        action: Literal["add", "remove"],
        user: int,
        item_kind: Literal["trials", "raids"],
        item_names: list[str],
    ) -> tuple[dict]:
        """Update entries for a user in the status table."""
        user_id = await self.get_user_from_discord_id(user)
        items = await self.get_item_ids(item_kind, item_names)
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
            WHERE user_id = ? AND item_kind = ? AND item_id = ?
        """
        for item in items:
            params = (has_item_entry, user_id, item_kind, item)
            await self.db_execute_qmark(query, params)

    async def check_table_shape(
        self: Self,
        table_name: Literal["users", "trials", "raids", "status"],
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
        table_name: Literal["trials", "raids"],
        expansion: None | str = None,
    ) -> list[str]:
        """Get a list of item names from a DB table."""
        table = await self.read_table_polars(table_name)
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
        kind: Literal["trials", "raids"],
    ) -> list[str]:
        """Get list of expansions in a table."""
        table = await self.read_table_polars(kind)
        return table.select("item_expac").to_series().unique().to_list()

    async def list_user_items(
        self: Self,
        user: int,
        check_type: Literal["has", "needs"],
        kind: Literal["trials", "raids"],
        expansion: str,
    ) -> list[str]:
        """Get list of mounts a user has or needs."""
        user_id = await self.get_user_from_discord_id(user)
        status_table = await self.read_table_polars("status")
        item_table = await self.read_table_polars(kind)
        check_val = 1 if check_type == "has" else 0
        status_table = (
            status_table.filter(
                (pl.col("user_id") == user_id)
                & (pl.col("item_kind") == kind)
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
        item_kind: Literal["trials", "raids"],
        old_name: str,
        new_name: str,
    ) -> None:
        """Edit an item name."""
        item_id = await self.get_item_ids(item_kind, old_name)
        if item_kind == "trials":
            query = "UPDATE trials SET item_name = ? WHERE item_id = ?"
        if item_kind == "raids":
            query = "UPDATE raids SET item_name = ? WHERE item_id = ?"
        params = (new_name, item_id[0])
        await self.db_execute_qmark(query, params)

    async def add_new_item(
        self: Self,
        kind: Literal["trials", "raids"],
        expansion: str,
        name: str,
    ) -> None:
        """Edit an item name."""
        new_row = self.create_item_row(name, expansion)
        if kind == "trials":
            await self.append_to_trial_table(new_row)
        if kind == "raids":
            await self.append_to_raid_table(new_row)

    async def delete_item(
        self: Self,
        kind: Literal["trials", "raids"],
        name: str,
    ) -> None:
        """Remove mounts from the database."""
        item_id = await self.get_item_ids(kind, name)
        params = tuple(item_id)
        if kind == "trials":
            query = "DELETE FROM trials WHERE item_id = ?"
        if kind == "raids":
            query = "DELETE FROM raids WHERE item_id = ?"
        await self.db_execute_qmark(query, params)

    async def delete_user(self: Self, name: str) -> None:
        """Remove mounts from the database."""
        user_id = await self.get_user_id(name)
        params = (user_id,)
        query = "DELETE FROM users WHERE user_id = ?"
        await self.db_execute_qmark(query, params)
