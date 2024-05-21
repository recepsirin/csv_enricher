from pathlib import Path

import aiofiles
import pytest
from aiocsv import AsyncDictReader, AsyncDictWriter

from csv_enricher.main import read_csv_async, write_data_to_csv


@pytest.mark.asyncio
async def test_read_csv_async(tmp_path: Path) -> None:
    test_csv_path = tmp_path / "test.csv"
    test_data = [{"lei": "1234567890", "isin": "FSDSFFDSFDSF"}, {"lei": "0987654321", "isin": "EZRFEREFFFE8"}]
    fieldnames = ["lei", "isin"]
    async with aiofiles.open(test_csv_path, mode="w", encoding="utf-8", newline="") as afp:
        writer = AsyncDictWriter(afp, fieldnames=fieldnames)
        await writer.writeheader()
        await writer.writerows(test_data)

    rows, lei_numbers = await read_csv_async(str(test_csv_path))

    assert rows == test_data
    assert lei_numbers == [row["lei"] for row in test_data]


@pytest.mark.asyncio
async def test_write_data_to_csv(tmp_path: Path) -> None:
    test_csv_path = tmp_path / "test.csv"
    test_data = [
        {
            "transaction_uti": "1030291281MARKITWIRE0000000000000112874138",
            "isin": "EZ9724VTXK48",
            "notional": "763000.0",
            "notional_currency": "GBP",
            "transaction_type": "Sell",
            "transaction_datetime": "2020-11-25T15:06:22Z",
            "rate": "0.0070956",
            "lei": "XKZZ2JZF41MRHTR1V493",
            "legalName": "Company A",
            "bic": "ABCDEF",
            "transaction_costs": "100.0",
        },
    ]

    await write_data_to_csv(str(test_csv_path), test_data)
    async with aiofiles.open(test_csv_path, mode="r", encoding="utf-8", newline="") as afp:
        rows = []
        async for row in AsyncDictReader(afp):
            rows.append(row)

    assert len(rows) == len(test_data)
    assert rows == test_data
