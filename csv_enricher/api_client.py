import configparser

import httpx


class APIClient:
    def _get_api_crendetials(self) -> tuple[str, str]:
        config = configparser.ConfigParser()
        config.read("api.ini")
        base_url = config["gleif"]["URL"]
        resource = config["gleif"]["RESOURCE"]
        return base_url, resource

    async def get_all_lei_records(self, lei: str | list[str]) -> httpx.Response:
        """Retrieve LEI Records by LEI (record resource identifier) or by filtering."""

        async with httpx.AsyncClient() as client:
            try:
                base_url, resource = self._get_api_crendetials()
                url = f"{base_url}/{resource}"
                if isinstance(lei, list):
                    lei = ",".join(lei)  # If lei is a list, combine them into a single string separated by commas
                params = {"filter[lei]": lei}  # Specify the correct filter format
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                raise exc
