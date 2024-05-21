from __future__ import annotations

from pathlib import Path

import aiocsv
import aiofiles
from aiofiles import os

from csv_enricher.logging_config import logger


async def get_files_in_directory(directory: str) -> list[str]:
    logger.info(f"Getting files in directory: {directory}")
    files = await os.listdir(directory)
    files.remove(".gitkeep") if ".gitkeep" in files else None
    logger.info(f"Found files: {files}")
    return files


async def clean_temp_csv_files(directory: str) -> None:
    """Delete all CSV files within a directory asynchronously."""
    logger.info(f"Cleaning temporary CSV files in directory: {directory}")
    directory_path = Path(directory)

    for item in directory_path.iterdir():
        # If it's a file and has a .csv extension, remove it
        if item.is_file() and item.suffix == ".csv":
            logger.info(f"Removing file: {item}")
            await aiofiles.os.remove(item)


async def split_csv_file(input_file: str, chunk_size: int) -> None:
    logger.info(f"Splitting CSV file: {input_file} into chunks of size: {chunk_size}")
    async with aiofiles.open(input_file, "r", newline="") as infile:
        reader = aiocsv.AsyncReader(infile)
        headers = await reader.__anext__()  # Read the headers first

        file_count = 1
        while True:
            output_file = f"_temp/chunks/{file_count}.csv"
            logger.info(f"Writing to output file: {output_file}")
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
                    logger.info("No more data to split or fewer rows than chunk size left")
                    return  # No more data to read or fewer rows than chunk_size left

            file_count += 1
            logger.info(f"Chunk {file_count-1} completed")
