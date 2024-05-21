from __future__ import annotations

from pathlib import Path

import aiocsv
import aiofiles
from aiofiles import os


async def get_files_in_directory(directory: str) -> list[str]:
    files = await os.listdir(directory)
    files.remove(".gitkeep") if ".gitkeep" in files else None
    return files


async def clean_temp_csv_files(directory: str) -> None:
    """Delete all CSV files within a directory asynchronously."""
    directory_path = Path(directory)

    for item in directory_path.iterdir():
        # If it's a file and has a .csv extension, remove it
        if item.is_file() and item.suffix == ".csv":
            await aiofiles.os.remove(item)


async def split_csv_file(input_file: str, chunk_size: int) -> None:
    async with aiofiles.open(input_file, "r", newline="") as infile:
        reader = aiocsv.AsyncReader(infile)
        headers = await reader.__anext__()  # Read the headers first

        file_count = 1
        while True:
            output_file = f"_temp/chunks/{file_count}.csv"
            async with aiofiles.open(output_file, "w", newline="") as outfile:
                writer = aiocsv.AsyncWriter(outfile)
                await writer.writerow(headers)  # Write the headers to each new file

                # Write up to chunk_size rows
                rows_written = 0
                async for row in reader:
                    await writer.writerow(row)
                    rows_written += 1
                    if rows_written >= chunk_size:
                        break

                if rows_written < chunk_size:
                    return  # No more data to read or fewer rows than chunk_size left

            file_count += 1
