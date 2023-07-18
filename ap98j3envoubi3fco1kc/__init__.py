import random
import aiohttp
from lxml import html
from typing import AsyncGenerator
import time
from datetime import datetime as datett
from datetime import timezone
import hashlib
import logging
from lxml.html import fromstring

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
)

import hashlib

global MAX_EXPIRATION_SECONDS
MAX_EXPIRATION_SECONDS = 60

subreddits = [
    "r/AlgorandOfficial",
    "r/almosthomeless",
    "r/altcoin",
    "r/amcstock",
    "r/Anarcho_Capitalism",
    "r/announcements",
    "r/announcements",
    "r/announcements",
    "r/announcements",
    "r/antiwork",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/asktrading",
    "r/Banking",
    "r/baseball",
    "r/binance",
    "r/Bitcoin",
    "r/Bitcoin",
    "r/Bitcoin",
    "r/bitcoin",
    "r/BitcoinBeginners",
    "r/Bitcoincash",
    "r/BitcoinMarkets",
    "r/books",
    "r/btc",
    "r/btc",
    "r/btc",
    "r/budget",
    "r/BullTrader",
    "r/Buttcoin",
    "r/cardano",
    "r/China",
    "r/CoinBase",
    "r/CreditCards",
    "r/Crypto",
    "r/Crypto_General",
    "r/Cryptocurrencies",
    "r/Cryptocurrencies",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrencyClassic",
    "r/CryptocurrencyMemes",
    "r/CryptoCurrencyTrading",
    "r/CryptoMarkets",
    "r/CryptoMoonShots",
    "r/CryptoMoonShots",
    "r/CryptoMarkets",
    "r/CryptoTechnology",
    "r/Damnthatsinteresting",
    "r/dataisbeautiful",
    "r/defi",
    "r/defi",
    "r/Dividends",
    "r/dogecoin",
    "r/dogecoin",
    "r/dogecoin",
    "r/dogecoin",
    "r/Economics",
    "r/Economics",
    "r/Economics",
    "r/eth",
    "r/ethereum",
    "r/ethereum",
    "r/ethereum",
    "r/ethereum",
    "r/ethermining",
    "r/ethfinance",
    "r/ethstaker",
    "r/ethtrader",
    "r/ethtrader",
    "r/ethtrader",
    "r/etoro",
    "r/etoro",
    "r/Europe",
    "r/facepalm",
    "r/facepalm",
    "r/fatFIRE",
    "r/Finance",
    "r/Finance",
    "r/Finance",
    "r/FinanceNews",
    "r/FinanceNews",
    "r/FinanceNews",
    "r/FinanceStudents",
    "r/FinancialCareers",
    "r/financialindependence",
    "r/FinancialPlanning",
    "r/financialplanning",
    "r/forex",
    "r/formula1",
    "r/france",
    "r/Frugal",
    "r/Futurology",
    "r/gaming",
    "r/Germany",
    "r/GME",
    "r/ico",
    "r/investing",
    "r/investor",
    "r/jobs",
    "r/leanfire",
    "r/ledgerwallet",
    "r/litecoin",
    "r/MiddleClassFinance",
    "r/Monero",
    "r/Monero",
    "r/nanocurrency",
    "r/NFT",
    "r/NoStupidQuestions",
    "r/passive_income",
    "r/pennystocks",
    "r/personalfinance",
    "r/PFtools",
    "r/politics",
    "r/politics",
    "r/politics",
    "r/povertyfinance",
    "r/povertyfinance",
    "r/povertyfinance",
    "r/realestateinvesting",
    "r/retirement",
    "r/Ripple",
    "r/robinhood",
    "r/robinhood",
    "r/Showerthoughts",
    "r/soccer",
    "r/space",
    "r/sports",
    "r/sports",
    "r/sports",
    "r/Stellar",
    "r/stockmarket",
    "r/stockmarket",
    "r/Stocks",
    "r/Stocks",
    "r/Stocks",
    "r/StudentLoans",
    "r/tax",
    "r/technicalraptor",
    "r/technology",
    "r/technology",
    "r/technology",
    "r/Tether",
    "r/todayilearned",
    "r/todayilearned",
    "r/todayilearned",
    "r/todayilearned",
    "r/trading",
    "r/trading",
    "r/trading",
    "r/tradingreligion",
    "r/unitedkingdom",
    "r/unpopularopinion",
    "r/ValueInvesting",
    "r/ValueInvesting",
    "r/ValueInvesting",
    "r/Wallstreet",
    "r/WallStreetBets",
    "r/WallStreetBets",
    "r/WallStreetBets",
    "r/WallStreetBetsCrypto",
    "r/Wallstreetsilver",
    "r/WhitePeopleTwitter",
    "r/WhitePeopleTwitter",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
]


async def find_random_subreddit_for_keyword(keyword: str = "BTC"):
    """
    Generate a subreddit URL using the search tool with `keyword`.
    It randomly chooses one of the resulting subreddit.
    """
    logging.info("[Pre-collect] generating Reddit target URL.")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://www.reddit.com/search/?q={keyword}&type=sr"
        ) as response:
            html_content = await response.text()
            tree = html.fromstring(html_content)
            urls = [
                url
                for url in tree.xpath('//a[contains(@href, "/r/")]//@href')
                if not "/r/popular" in url
            ]
            result = f"https://old.reddit.com{random.choice(urls)}/new"
            return result


async def generate_url(autonomous_subreddit_choice=0.33, keyword: str = "BTC"):
    random_value = random.random()
    if random_value < autonomous_subreddit_choice:
        return await find_random_subreddit_for_keyword(keyword)
    else:
        logging.info("[Reddit] Top 100 Subreddits mode!")
        return "https://reddit.com/" + random.choice(subreddits)


def is_within_timeframe_seconds(input_timestamp, timeframe_sec):
    input_timestamp = int(input_timestamp)
    current_timestamp = int(time.time())  # Get the current UNIX timestamp
    elapsed_time = current_timestamp - input_timestamp

    if elapsed_time <= timeframe_sec:
        return True
    else:
        return False


def format_timestamp(timestamp):
    dt = datett.fromtimestamp(timestamp, timezone.utc)
    formatted_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_timestamp


async def scrap_post(url: str) -> AsyncGenerator[Item, None]:
    resolvers = {}

    async def post(data) -> AsyncGenerator[Item, None]:
        """t3"""
        content = data["data"]
        item_ = Item(
            content=Content(content["selftext"]),
            author=Author(
                hashlib.sha1(
                    bytes(content["author"], encoding="utf-8")
                ).hexdigest()
            ),
            created_at=CreatedAt(
                str(format_timestamp(content["created_utc"]))
            ),
            title=Title(content["title"]),
            domain=Domain("reddit.com"),
            url=Url("https://reddit.com" + content["url"]),
        )
        if is_within_timeframe_seconds(
            content["created_utc"], MAX_EXPIRATION_SECONDS
        ):
            yield item_

    async def comment(data) -> AsyncGenerator[Item, None]:
        """t1"""
        content = data["data"]
        item_ = Item(
            content=Content(content["body"]),
            author=Author(
                hashlib.sha1(
                    bytes(content["author"], encoding="utf-8")
                ).hexdigest()
            ),
            created_at=CreatedAt(
                str(format_timestamp(content["created_utc"]))
            ),
            domain=Domain("reddit.com"),
            url=Url("https://reddit.com" + content["permalink"]),
        )
        if is_within_timeframe_seconds(
            content["created_utc"], MAX_EXPIRATION_SECONDS
        ):
            yield item_

    async def more(__data__):
        for __item__ in []:
            yield Item()

    async def kind(data) -> AsyncGenerator[Item, None]:
        if not isinstance(data, dict):
            return
        resolver = resolvers.get(data["kind"], None)
        if not resolver:
            raise NotImplementedError(f"{data['kind']} is not implemented")
        try:
            async for item in resolver(data):
                yield item
        except Exception as err:
            raise err

    async def listing(data) -> AsyncGenerator[Item, None]:
        for item_data in data["data"]["children"]:
            async for item in kind(item_data):
                yield item

    resolvers = {"Listing": listing, "t1": comment, "t3": post, "more": more}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url + ".json") as response:
                [post, comments] = await response.json()
                async for result in kind(post):
                    yield result
                async for commentary in kind(comments):
                    yield commentary
    except Exception:
        pass

async def scrap_subreddit(subreddit_url: str) -> AsyncGenerator[Item, None]:
    async with aiohttp.ClientSession() as session:
        url_to_fetch = subreddit_url
        async with session.get(url_to_fetch) as response:
            html_content = await response.text()
            html_tree = fromstring(html_content)
            for post in html_tree.xpath("//div[@data-testid='post-container']"):
                sublinks = post.xpath(".//a[starts-with(@href, '/r/')]/@href")
                sublink = list(set(sublinks))
                sublink_found = None
                for sublink in sublinks:
                    if "comments" in sublink:
                        sublink_found = sublink
                        break
                subreddit_scrap_URL = "https://reddit.com" + sublink_found
                async for item in scrap_post(
                    subreddit_scrap_URL
                ):
                    yield item


DEFAULT_OLDNESS_SECONDS = 30
DEFAULT_MAXIMUM_ITEMS = 15
DEFAULT_MIN_POST_LENGTH = 10


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS
            )
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS
            )
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get(
                "min_post_length", DEFAULT_MIN_POST_LENGTH
            )
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    global MAX_EXPIRATION_SECONDS
    (
        max_oldness_seconds,
        MAXIMUM_ITEMS_TO_COLLECT,
        min_post_length,
    ) = read_parameters(parameters)
    MAX_EXPIRATION_SECONDS = max_oldness_seconds
    url = await generate_url(**parameters["url_parameters"])
    logging.info("[Reddit] Scraping %s", url)
    if "reddit.com" not in url:
        raise ValueError(f"Not a Reddit URL {url}")
    url_parameters = url.split("reddit.com")[1].split("/")[1:]
    if "comments" in url_parameters:
        async for result in scrap_post(url):
            logging.info("[Reddit] found post = %s", result)
            yield result
    else:
        async for result in scrap_subreddit(url):
            logging.info("[Reddit] found post = %s", result)
            yield result
