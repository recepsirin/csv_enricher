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


async def read_csv_async(file_path: str) -> tuple[list[dict], list[str]]:
    lei_numbers = []
    rows = []
    async with aiofiles.open(file_path, mode="r", encoding="utf-8", newline="") as afp:
        async for row in AsyncDictReader(afp):
            lei_numbers.append(row["lei"])
            rows.append(row)
    return rows, lei_numbers


async def enrich(rows: list[dict], lei_numbers: list[str] | str) -> list:
    response = await APIClient().get_all_lei_records(lei_numbers)
    if response.status_code != 200:
        # possibly can be retried ... not succeeded
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

                elif record["country"] == "NL":
                    row["transaction_costs"] = abs(
                        (float(row["notional"]) * (1 / float(row["rate"]))) - float(row["notional"])
                    )
                else:
                    row["transaction_costs"] = "N/A"

    return rows


async def write_data_to_csv(file_path: str, data: list[dict[str, Any]]) -> None:
    file_exists = await os.path.isfile(file_path)
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


async def main(file_to_be_enriched: str) -> None:
    await split_csv_file(file_to_be_enriched, 500)  # so far most efficient option
    files = await get_files_in_directory("_temp/chunks/")
    sorted_files = sorted(files, key=lambda x: int(x.split(".")[0]))
    for file in sorted_files:
        rows_list, leis = await read_csv_async(f"_temp/chunks/{file}")
        enriched_rows = await enrich(rows_list, leis)
        await write_data_to_csv("output.csv", enriched_rows)

    await clean_temp_csv_files("_temp/chunks/")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process a CSV file.")
    parser.add_argument("filepath", type=str, help="The path to the CSV file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.filepath))
