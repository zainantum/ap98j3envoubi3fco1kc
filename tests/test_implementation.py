from ap98j3envoubi3fco1kc import query
from exorde_data import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    results = []
    async for result in query(
        {
            "url_parameters": {
                "keyword": "BTC",
                "autonomous_subreddit_choice": 1,
            }
        }
    ):
        assert isinstance(result, Item)
        results.append(result)
    print(len(results))
