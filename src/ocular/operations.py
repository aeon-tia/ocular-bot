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
            {
                "item_id": "1ef80ff70ffd6960b01abb534c65c291",
                "item_name": "ifrit",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffa63336c2991c31f00cd8dd05b",
                "item_name": "titan",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffa890e6a46a98f77df0bb46b2c",
                "item_name": "garuda",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffaa96867e6a2d02eb27278b71b",
                "item_name": "leviathan",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffacb066434bd0424dc8fe8a7eb",
                "item_name": "ramuh",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffaf60a60dcb7bad86156499d73",
                "item_name": "shiva",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef80ffaf83a6007afbd82b6a222010a",
                "item_name": "bismark",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffb505c6162857184d477996934",
                "item_name": "ravana",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffb76236079a919fa5ae59fd65f",
                "item_name": "thordan",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffba5e568e5b111370abb00cce8",
                "item_name": "sephirot",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffbd7f86162b1c56eb04b60c1c0",
                "item_name": "nidhogg",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffbfe1367a4a20eeedcfb31054f",
                "item_name": "sophia",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffc2dde6e9db8d6f0f24ae42805",
                "item_name": "zurvan",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef80ffc5681618a9774c212295a64ae",
                "item_name": "susano",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffc76516bc3b9cebbdf6c6bb413",
                "item_name": "lakshmi",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffc95306887b4a9183dbb17d347",
                "item_name": "shinryu",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffcb84862a089d6e9194504b110",
                "item_name": "byakko",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffce3856339a756f3c070d68d43",
                "item_name": "tsukuyomi",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffd08be6ba29baeb490dd052ead",
                "item_name": "suzaku",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffd2eb96d2c9598fafb76925b39",
                "item_name": "seiryu",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef80ffd515e67e3b33a8dbadfcc6b94",
                "item_name": "titania",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffd746d6c409b0de4d89a034e4a",
                "item_name": "innocence",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffd9f5c6747ab3286754dda6e86",
                "item_name": "hades",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffdc2b26d4bb95bac59aa95d24f",
                "item_name": "warrior of light",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffde76064caae966498d2afe780",
                "item_name": "ruby",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffe0d9b6bdd8d44335b1818ed5d",
                "item_name": "emerald",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffe4298659e978aa7d2a1bf0fdf",
                "item_name": "diamond",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef80ffe79ee68d5806f0712c2f8cc23",
                "item_name": "zodiark",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80ffea0a166d7b6194c0b31411a3f",
                "item_name": "hydaelyn",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80ffee8dd64f7b182a494cbd76ced",
                "item_name": "endsinger",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80fff0fe8683b9911b093c4d519aa",
                "item_name": "barbariccia",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80fff33626d1cbd8c7933467142fd",
                "item_name": "rubicante",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80fff59e768d9af42c3daf49bbe39",
                "item_name": "golbez",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80fff7a0163dba7162e4940e5b5c5",
                "item_name": "zeromus",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef80fffa28f6a4682772c5db44f322d",
                "item_name": "valigarmanda",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef80fffcfbb6c799d37ae1f2bb1d0f8",
                "item_name": "zoraal ja",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef80ffffe13647da64e20f17c5c1705",
                "item_name": "ex3",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef8100022d46b74935baeadef79390a",
                "item_name": "ex4",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef8100047736b5fac70fb51f2457913",
                "item_name": "ex5",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef8100085da6e36a9f834a6e987df45",
                "item_name": "ex6",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef81000a370618f89ddb7962bf566a9",
                "item_name": "ex7",
                "item_expac": "dawntrail",
            },
        )
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
        rows = (
            {
                "item_id": "1ef81000d4c36649b84f4ae79a73c4e3",
                "item_name": "t5",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef81000f621617694ba05b091bc104f",
                "item_name": "t9",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef810011ee06ec9bde84b794f1da23e",
                "item_name": "t13",
                "item_expac": "a realm reborn",
            },
            {
                "item_id": "1ef8100146226255aa395042061349c5",
                "item_name": "a4s",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef81001674f606c9310b74e20d7eec3",
                "item_name": "a8s",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef8100189cc60e9a7ad6b069c1d4dcf",
                "item_name": "a12s",
                "item_expac": "heavensward",
            },
            {
                "item_id": "1ef81001aec2687fa4f5cc5fb3627eb8",
                "item_name": "o4s",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef81001d4ab662e8cdc48c6f2c08504",
                "item_name": "o8s",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef8100202806fecac32a3d3f5c6a4be",
                "item_name": "o12s",
                "item_expac": "stormblood",
            },
            {
                "item_id": "1ef8100227c56063a8335d4cef6ea9d2",
                "item_name": "e4s",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef8100249f76bdcbb2d8baf0afa2139",
                "item_name": "e8s",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef810026de064a7971e2a8145de4585",
                "item_name": "e12s",
                "item_expac": "shadowbringers",
            },
            {
                "item_id": "1ef8100293886c2a81feddefd16a427e",
                "item_name": "p4s",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef81002b6ed627aa66d7c9192129a41",
                "item_name": "p8s",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef81002ddb76acbb2aefe536aff7112",
                "item_name": "p12s",
                "item_expac": "endwalker",
            },
            {
                "item_id": "1ef81002fe9168b297564a08de8aab21",
                "item_name": "m4s",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef81003215e6a58a071afd835bf5bad",
                "item_name": "m8s",
                "item_expac": "dawntrail",
            },
            {
                "item_id": "1ef8100346576365a7bb56b58babea2b",
                "item_name": "m12s",
                "item_expac": "dawntrail",
            },
        )
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
        check_col: Literal["user_name", "user_id", "discord_id"],
        check_val: str | int,
    ) -> bool:
        """Check if a user already exists in the users table."""
        table = await self.read_table_polars("users")
        if table.shape[0] != 0:
            table_empty = not (
                table.filter(pl.col(check_col) == check_val)
                .select("user_id")
                .is_empty()
            )
            return not table_empty
        return False

    async def list_item_names(
        self: Self,
        table_name: Literal["trials", "raids"],
        expansion_name: None
        | Literal[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalker",
            "dawntrail",
        ] = None,
    ) -> list[str]:
        """Get a list of item names from a DB table."""
        table = await self.read_table_polars(table_name)
        if expansion_name is None:
            return table.select("item_name").to_series().to_list()
        return (
            table.filter(pl.col("item_expac") == expansion_name)
            .select("item_name")
            .to_series()
            .to_list()
        )

    async def list_user_items(
        self: Self,
        user: int,
        check_type: Literal["has", "needs"],
        kind: Literal["trials", "raids"],
        expansion: Literal[
            "a realm reborn",
            "heavensward",
            "stormblood",
            "shadowbringers",
            "endwalker",
            "dawntrail",
        ],
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
        item_names =  (
            status_table.join(item_table, on="item_id", how="inner")
            .filter(pl.col("item_expac") == expansion)
            .select("item_name")
            .to_series()
            .to_list()
        )
        if len(item_names) == 0:
            return ["none"]
        return item_names
