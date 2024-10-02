"""Database operations for discord bot."""

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
        """Execute a DB query with the qmarks placeholder syntax."""
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
        rows = (
            {"item_id": 0, "item_name": "ifrit"},
            {"item_id": 1, "item_name": "titan"},
            {"item_id": 2, "item_name": "garuda"},
            {"item_id": 3, "item_name": "leviathan"},
            {"item_id": 4, "item_name": "ramuh"},
            {"item_id": 5, "item_name": "shiva"},
            {"item_id": 6, "item_name": "bismark"},
            {"item_id": 7, "item_name": "ravana"},
            {"item_id": 8, "item_name": "thordan"},
            {"item_id": 9, "item_name": "sephirot"},
            {"item_id": 10, "item_name": "nidhogg"},
            {"item_id": 11, "item_name": "sophia"},
            {"item_id": 12, "item_name": "zurvan"},
            {"item_id": 13, "item_name": "susano"},
            {"item_id": 14, "item_name": "lakshmi"},
            {"item_id": 15, "item_name": "shinryu"},
            {"item_id": 16, "item_name": "byakko"},
            {"item_id": 17, "item_name": "tsukuyomi"},
            {"item_id": 18, "item_name": "suzaku"},
            {"item_id": 19, "item_name": "seiryu"},
            {"item_id": 20, "item_name": "titania"},
            {"item_id": 21, "item_name": "innocence"},
            {"item_id": 22, "item_name": "hades"},
            {"item_id": 23, "item_name": "warrior of light"},
            {"item_id": 24, "item_name": "ruby"},
            {"item_id": 25, "item_name": "emerald"},
            {"item_id": 26, "item_name": "diamond"},
            {"item_id": 27, "item_name": "zodiark"},
            {"item_id": 28, "item_name": "hydaelyn"},
            {"item_id": 29, "item_name": "endsinger"},
            {"item_id": 30, "item_name": "barbariccia"},
            {"item_id": 31, "item_name": "rubicante"},
            {"item_id": 32, "item_name": "golbez"},
            {"item_id": 33, "item_name": "zeromus"},
            {"item_id": 34, "item_name": "valigarmanda"},
            {"item_id": 35, "item_name": "zoraal ja"},
            {"item_id": 36, "item_name": "ex3"},
            {"item_id": 37, "item_name": "ex4"},
            {"item_id": 38, "item_name": "ex5"},
            {"item_id": 39, "item_name": "ex6"},
            {"item_id": 40, "item_name": "ex7"},
        )
        query = """
            CREATE TABLE IF NOT EXISTS trials(item_id INTEGER, item_name STRING)
        """
        await self.db_execute_literal(query)
        await self.append_to_trial_table(rows)

    async def init_raid_table(self: Self) -> tuple[dict]:
        """Create rows for initializing raid table."""
        rows = (
            {"item_id": 0, "item_name": "t5"},
            {"item_id": 1, "item_name": "t9"},
            {"item_id": 2, "item_name": "t13"},
            {"item_id": 3, "item_name": "a4s"},
            {"item_id": 4, "item_name": "a8s"},
            {"item_id": 5, "item_name": "a12s"},
            {"item_id": 6, "item_name": "o4s"},
            {"item_id": 7, "item_name": "o8s"},
            {"item_id": 8, "item_name": "o12s"},
            {"item_id": 9, "item_name": "e4s"},
            {"item_id": 10, "item_name": "e8s"},
            {"item_id": 11, "item_name": "e12s"},
            {"item_id": 12, "item_name": "p4s"},
            {"item_id": 13, "item_name": "p8s"},
            {"item_id": 14, "item_name": "p12s"},
            {"item_id": 15, "item_name": "m4s"},
            {"item_id": 16, "item_name": "m8s"},
            {"item_id": 17, "item_name": "m12s"},
        )
        query = """
            CREATE TABLE IF NOT EXISTS raids(item_id INTEGER, item_name STRING)
        """
        await self.db_execute_literal(query)
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
                item_id INTEGER,
                item_kind STRING,
                has_item INTEGER
            )
        """
        await self.db_execute_literal(query)

    async def init_tables(self: Self) -> None:
        """Initialize database tables."""
        data_dir = Path("./data")
        if not data_dir.is_dir():
            data_dir.mkdir()
        await self.init_user_table()
        await self.init_raid_table()
        await self.init_trial_table()
        await self.init_status_table()

    def create_user_row(self: Self, name: str, discord_id: int) -> tuple[dict]:
        """Create a new user ID as a row for the user table."""
        new_id = uuid6.uuid7().hex
        return ({"user_name": name, "user_id": new_id, "user_discord_id": discord_id},)

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
        query = "INSERT INTO trials VALUES(:item_id, :item_name)"
        await self.db_execute_dictuple(query, new_rows)

    async def append_to_raid_table(self: Self, new_rows: tuple[dict]) -> None:
        """Add new raids to the raids table.

        Parameters
        ----------
        new_rows : tuple[dict]
            Rows to append to raid table. Must be keyed by item_id and
            item_name. Each new row should be a separate dict.

        """
        query = "INSERT INTO raids VALUES(:item_id, :item_name)"
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

    async def append_new_user_status(self: Self, user_name: str) -> tuple[dict]:
        """Create status table rows for a new user."""
        user = await self.get_user_id(user_name)
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
        user_name: str,
        item_kind: Literal["trials", "raids"],
        item_names: list[str],
    ) -> tuple[dict]:
        """Update entries for a user in the status table."""
        user = await self.get_user_id(user_name)
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
            params = (has_item_entry, user, item_kind, item)
            await self.db_execute_qmark(query, params)
