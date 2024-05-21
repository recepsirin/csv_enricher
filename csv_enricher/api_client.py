import configparser

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random

from csv_enricher.logging_config import logger


class APIClient:
    def _get_api_crendetials(self) -> tuple[str, str]:
        logger.info("Reading API credentials from configuration file")
        config = configparser.ConfigParser()
        config.read("api.ini")
        base_url = config["gleif"]["URL"]
        resource = config["gleif"]["RESOURCE"]
        logger.info(f"Base URL: {base_url}, Resource: {resource}")
        return base_url, resource

    @retry(stop=stop_after_attempt(3), retry=retry_if_exception(httpx.HTTPStatusError), wait=wait_random(min=1, max=5))
    async def get_all_lei_records(self, lei: str | list[str]) -> httpx.Response:
        """Retrieve LEI Records by LEI (record resource identifier) or by filtering."""

        async with httpx.AsyncClient() as client:
            try:
                base_url, resource = self._get_api_crendetials()
                url = f"{base_url}/{resource}"
                if isinstance(lei, list):
                    lei = ",".join(lei)  # If lei is a list, combine them into a single string separated by commas
                params = {"filter[lei]": lei}  # Specify the correct filter format
                response = await client.get(url, params=params, timeout=60.0)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                logger.error(f"HTTP error occurred: {exc}")
                raise exc
