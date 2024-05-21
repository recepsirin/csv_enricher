# CSV Enricher

## Table of Contents

1. [Introduction](#installation)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Build](#build)
5. [Style](#style)



### Introduction

This application processes a single CSV file at a time by reading it and splitting it into smaller chunks, each containing 500 records.
This manual chunk size optimization is combined with the use of asyncio and aiofiles libraries. It ensures that the operation is performed efficiently.
By processing these smaller chunks asynchronously, the application minimizes resource usage and effectively handles large datasets(currently it is tested up to 10k rows).
The data in each chunk is enriched using information retrieved asynchronously from an external API. While enriching the files, I/O operations do not block the program's execution.
This approach prevents potential inaccuracies that may arise from simultaneously reading and updating the same file.
Once all chunks are processed, the enriched data is consolidated into a new CSV file, the application preserves the integrity of the original while providing an enriched dataset.
The application logs various events and operations throughout the process, ensuring transparency and facilitating error detection and resolution.
Upon completion, it displays a colorful message to the user, signaling the successful enrichment of the CSV file and indicating where the updated data can be found.



### Installation


To create a virtual environment
```bash
poetry shell
```

To install dependencies
```bash
poetry install
```

To run tests
```bash
poetry run pytest
```

### Usage

To run the application
```bash
poetry run python csv_enricher/main.py file_directory/input.csv
```

To get help
```bash
poetry run python csv_enricher/main.py -h
```


### Build

If you want to build the application as a package via poetry
```bash
poetry build
```


### Style

**Black**: Black is used for code formatting.
**isort**: isort is used for importing sorting.
**mypy**: mypy is used for static type checking.
**flake8**: flake8 is used as a code linter.
**pre-commit**: pre-commit is used to automatically run the above tools before each commit.
