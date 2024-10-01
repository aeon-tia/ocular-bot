"""Tests for the ocular bot's DB operations module."""
from pathlib import Path
from typing import Self

import polars as pl
import pytest

from src.ocular.operations import DataBase


class TestOperations:
    """Class with test methods for the DB operations."""

    @pytest.mark.asyncio
    async def test_init_trial_table(self: Self) -> None:
        """Test trial table initialization."""
        tempdb = Path("./data/temp.db")
        database = DataBase()
        database.db_path = tempdb
        await database.init_tables()
        table = pl.DataFrame(await database.get_trial_table())
        assert isinstance(table, pl.DataFrame)
        assert table.shape == (table.select("item_id").max().item()+1, 2)
        assert table.columns == ["item_id", "item_name"]
        assert table.dtypes == [pl.Int64, pl.String]
        assert table.select("item_id").is_unique().all()
        assert table.select("item_name").is_unique().all()
        tempdb.unlink()

    @pytest.mark.asyncio
    async def test_init_raid_table(self: Self) -> None:
        """Test raid table initialization."""
        tempdb = Path("./data/temp.db")
        database = DataBase()
        database.db_path = tempdb
        await database.init_tables()
        table = pl.DataFrame(await database.get_raid_table())
        assert isinstance(table, pl.DataFrame)
        assert table.shape == (table.select("item_id").max().item()+1, 2)
        assert table.columns == ["item_id", "item_name"]
        assert table.dtypes == [pl.Int64, pl.String]
        assert table.select("item_id").is_unique().all()
        assert table.select("item_name").is_unique().all()
        tempdb.unlink()

    def test_create_user_entry(self: Self) -> None:
        """Test user ID creation."""
        database = DataBase()
        usr_id = database.create_user_row(name="test", discord_id=0)
        assert isinstance(usr_id, tuple)
        assert len(usr_id[0]) == 3  # noqa: PLR2004
