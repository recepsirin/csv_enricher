import asyncio
import os
import tempfile
from tempfile import TemporaryDirectory
from typing import Iterator

import aiocsv
import aiofiles
import pytest

from csv_enricher.functions import clean_temp_csv_files, get_files_in_directory, split_csv_file


@pytest.fixture
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_directory() -> Iterator[TemporaryDirectory]:
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.mark.asyncio
async def test_get_files_in_directory(temp_directory: TemporaryDirectory) -> None:
    file_names = ["1.csv", "2.csv", "3.csv"]
    for file_name in file_names:
        file_path = os.path.join(temp_directory, file_name)
        with open(file_path, "w") as f:
            f.write("Test content")

    # Test getting files in the directory
    files = await get_files_in_directory(temp_directory)
    assert len(files) == 3
    assert "1.csv" in files
    assert "2.csv" in files
    assert "3.csv" in files


@pytest.mark.asyncio
async def test_clean_temp_csv_files(temp_directory: TemporaryDirectory) -> None:
    csv_files = ["1.csv", "2.txt", "3.csv"]
    for file_name in csv_files:
        file_path = os.path.join(temp_directory, file_name)
        with open(file_path, "w") as f:
            f.write("Test content")

    await clean_temp_csv_files(temp_directory)

    remaining_files = os.listdir(temp_directory)
    assert len(remaining_files) == 1  # Only non-CSV file should remain
    assert "2.txt" in remaining_files


@pytest.mark.asyncio
async def test_split_csv_file() -> None:
    # Create a temporary CSV file with some sample data
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write("lei, isin\n")
        temp_file.write("BP2XS5X7210Y05NIXW11, EZ19M089SB39\n")
        temp_file.write("DEW9FIF7210Y05NIFW22, KZ20M089SB39\n")
        temp_file.write("WEOJDDS7210Y05FSDF00, QQF20M089SBF9\n")
        temp_file.write("XXXXXDS7210Y08889999, 3242343289SB39\n")

    chunk_size = 2

    try:
        await split_csv_file(temp_file.name, chunk_size)

        expected_chunks = [
            [["lei", " isin"], ["BP2XS5X7210Y05NIXW11", " EZ19M089SB39"], ["DEW9FIF7210Y05NIFW22", " KZ20M089SB39"]],
            # Chunk 1
            [["lei", " isin"], ["WEOJDDS7210Y05FSDF00", " QQF20M089SBF9"], ["XXXXXDS7210Y08889999", " 3242343289SB39"]],
            # Chunk 2
        ]
        for file_count, expected_content in enumerate(expected_chunks, start=1):
            output_file = f"_temp/chunks/{file_count}.csv"
            assert os.path.exists(output_file)

            # Read the chunk file and verify its content
            async with aiofiles.open(output_file, "r", newline="") as infile:
                reader = aiocsv.AsyncReader(infile)
                rows = [row async for row in reader]
                assert rows == expected_content

    finally:
        # Clean up temporary files
        os.remove(temp_file.name)
        for file_count in range(4):
            output_file = f"_temp/chunks/{file_count}.csv"
            if os.path.exists(output_file):
                os.remove(output_file)
