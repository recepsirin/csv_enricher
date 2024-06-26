from __future__ import annotations

import argparse
import asyncio
import csv
from typing import Any

import aiofiles
from aiocsv import AsyncDictReader, AsyncDictWriter
from aiofiles import os

from csv_enricher.api_client import APIClient
from csv_enricher.functions import clean_temp_csv_files, get_files_in_directory, split_csv_file
from csv_enricher.logging_config import logger


async def read_csv_async(file_path: str) -> tuple[list[dict], list[str]]:
    lei_numbers = []
    rows = []
    logger.info(f"Reading CSV file: {file_path}")
    async with aiofiles.open(file_path, mode="r", encoding="utf-8", newline="") as afp:
        async for row in AsyncDictReader(afp):
            lei_numbers.append(row["lei"])
            rows.append(row)
    logger.info(f"Finished reading CSV file: {file_path}")
    return rows, lei_numbers


async def enrich(rows: list[dict], lei_numbers: list[str] | str) -> list:
    logger.info(f"Enriching data for LEI numbers: {lei_numbers}")
    response = await APIClient().get_all_lei_records(lei_numbers)
    if response.status_code != 200:
        logger.error(f"Failed to get LEI records: {response.status_code}")
        return []
    collection = []

    for lei_record in response.json()["data"]:
        enrichment_container = {
            "lei": lei_record["attributes"]["lei"],
            "name": lei_record["attributes"]["entity"]["legalName"]["name"],
            "bic": lei_record["attributes"]["bic"],
            "country": lei_record["attributes"]["entity"]["legalAddress"]["country"],
        }
        collection.append(enrichment_container)
    for record in collection:
        for row in rows:
            if record["lei"] == row["lei"]:
                row["legalName"] = record["name"]
                row["bic"] = ", ".join(record["bic"])
                if record["country"] == "GB":  # add check to validate either notional or rate is 0
                    row["transaction_costs"] = (float(row["notional"]) * float(row["rate"])) - float(row["notional"])

                elif record["country"] == "NL" and row["rate"] != 0:
                    row["transaction_costs"] = abs(
                        (float(row["notional"]) * (1 / float(row["rate"]))) - float(row["notional"])
                    )
                else:
                    row["transaction_costs"] = "N/A"

    logger.info(f"Finished enriching data for LEI numbers: {lei_numbers}")
    return rows


async def write_data_to_csv(file_path: str, data: list[dict[str, Any]]) -> None:
    file_exists = await os.path.isfile(file_path)
    logger.info(f"Writing data to CSV file: {file_path}")
    async with aiofiles.open(file_path, mode="a", encoding="utf-8", newline="") as afp:
        writer = AsyncDictWriter(
            afp,
            fieldnames=[
                "transaction_uti",
                "isin",
                "notional",
                "notional_currency",
                "transaction_type",
                "transaction_datetime",
                "rate",
                "lei",
                "legalName",
                "bic",
                "transaction_costs",
            ],
            restval="NULL",
            quoting=csv.QUOTE_ALL,
        )
        if not file_exists:
            await writer.writeheader()
        await writer.writerows(data)
    logger.info(f"Finished writing data to CSV file: {file_path}")


async def main(file_to_be_enriched: str) -> None:
    logger.info(f"Starting process for file: {file_to_be_enriched}")
    await split_csv_file(file_to_be_enriched, 500)  # so far most efficient option
    files = await get_files_in_directory("_temp/chunks/")
    sorted_files = sorted(files, key=lambda x: int(x.split(".")[0]))
    for file in sorted_files:
        rows_list, leis = await read_csv_async(f"_temp/chunks/{file}")
        enriched_rows = await enrich(rows_list, leis)
        await write_data_to_csv("output.csv", enriched_rows)

    await clean_temp_csv_files("_temp/chunks/")
    logger.info(f"Finished process for file: {file_to_be_enriched}")


async def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process a CSV file.")
    parser.add_argument("filepath", type=str, help="The path to the CSV file")
    args = parser.parse_args()

    if not args.filepath.lower().endswith(".csv"):
        parser.error("The provided file must have a .csv extension.")

    if not await aiofiles.os.path.isfile(args.filepath):
        parser.error(f"The provided file '{args.filepath}' does not exist.")

    return args


async def run() -> None:
    try:
        args = await parse_args()
        print("Starting the CSV enrichment process")
        await main(args.filepath)
        print("\033[92mCSV enrichment process completed.\033[0m You can find the output file in the current directory.")
    except argparse.ArgumentError as e:
        print(e)
    except Exception as e:
        print("\033[91mAn error occurred during CSV enrichment process:", e, "\033[0m")


if __name__ == "__main__":
    asyncio.run(run())
