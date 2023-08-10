import random
import aiohttp
import asyncio
from lxml import html
from typing import AsyncGenerator
import time
from datetime import datetime as datett
from datetime import timezone
import hashlib
import logging
from lxml.html import fromstring
import re
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
from wordsegment import load, segment
load()

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
]

global MAX_EXPIRATION_SECONDS
global SKIP_POST_PROBABILITY
MAX_EXPIRATION_SECONDS = 80000
SKIP_POST_PROBABILITY = 0.1
BASE_TIMEOUT = 30

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
    ###
    "r/BaldursGate3",
    "r/teenagers",
    "r/BigBrother",
    "r/BigBrother",
    "r/BigBrother",
    "r/wallstreetbets",
    "r/wallstreetbets",
    "r/namenerds",
    "r/Eldenring",
    "r/Unexpected",
    "r/NonCredibleDefense",
    "r/wallstreetbets",
    "r/news",
    "r/news",
    "r/news",
    "r/mildlyinteresting",  
    "r/RandomThoughts",
    "r/ireland",
    "r/france",
    "r/ireland",
    "r/de",
    "r/ireland",
    "r/unitedkingdom", "r/AskUK", "r/CasualUK", "r/britishproblems",
    "r/canada", "r/AskCanada", "r/onguardforthee", "r/CanadaPolitics",
    "r/australia", "r/AskAnAustralian", "r/straya", "r/sydney",
    "r/india", "r/AskIndia", "r/bollywood", "r/Cricket",
    "r/germany", "r/de", "r/LearnGerman", "r/germusic",
    "r/france", "r/French", "r/paris", "r/europe",
    "r/japan", "r/japanlife", "r/newsokur", "r/learnjapanese",
    "r/brasil", "r/brasilivre", "r/riodejaneiro", "r/saopaulo",
    "r/mexico", "r/MexicoCity", "r/spanish", "r/yo_espanol",
    # 50 Most Popular News, Politics, and Finance/Economics Subreddits
    "r/news", "r/worldnews", "r/UpliftingNews", "r/nottheonion", "r/TrueReddit",
    "r/politics", "r/PoliticalDiscussion", "r/worldpolitics", "r/neutralpolitics", "r/Ask_Politics",
    "r/personalfinance", "r/investing", "r/StockMarket", "r/financialindependence", "r/economics",
    # 50 Simply Relevant/Popular Subreddits
    "r/AskReddit", "r/IAmA", "r/funny", "r/pics", "r/gaming", "r/aww", "r/todayilearned",
    "r/science", "r/technology", "r/worldnews", "r/Showerthoughts", "r/books", "r/movies",
    "r/Music", "r/Art", "r/history", "r/EarthPorn", "r/food", "r/travel", "r/fitness", "r/DIY",
    "r/LifeProTips", "r/explainlikeimfive", "r/dataisbeautiful", "r/futurology", "r/WritingPrompts",
    "r/nosleep", "r/personalfinance", "r/photography", "r/NatureIsFuckingLit", "r/Advice",
    "r/askscience", "r/gadgets", "r/funny", "r/pics", "r/gaming", "r/aww", "r/todayilearned",
    "r/science", "r/technology", "r/worldnews", "r/Showerthoughts", "r/books", "r/movies",
    "r/Music", "r/Art", "r/history", "r/EarthPorn", "r/food", "r/travel", "r/fitness", "r/DIY",
    "r/LifeProTips", "r/explainlikeimfive", "r/dataisbeautiful", "r/futurology", "r/WritingPrompts"
]


async def find_random_subreddit_for_keyword(keyword: str = "BTC"):
    """
    Generate a subreddit URL using the search tool with `keyword`.
    It randomly chooses one of the resulting subreddit.
    """
    logging.info("[Reddit] generating subreddit target URL.")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://www.reddit.com/search/?q={keyword}&type=sr",
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},              
                timeout = BASE_TIMEOUT
            ) as response:
                html_content = await response.text()
                tree = html.fromstring(html_content)
                urls = [
                    url
                    for url in tree.xpath('//a[contains(@href, "/r/")]//@href')
                    if not "/r/popular" in url
                ]
                result = f"https:/reddit.com{random.choice(urls)}/new"
                return result
    finally:
        await session.close()


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

def extract_subreddit_name(input_string):
    match = re.search(r'r/([^/]+)', input_string)
    if match:
        return match.group(1)
    return None

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
            _url = url + ".json"
            logging.info(f"[Reddit] Scraping - getting {_url}")
            async with session.get(_url, 
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},     
                timeout=BASE_TIMEOUT) as response:
                response = await response.json()
                [_post, comments] = response
                try:
                    async for item in kind(_post):
                        yield (item)
                except GeneratorExit:
                    logging.info("[Reddit] Scraper generator exit...")
                    return
                except:
                    logging.exception(f"An error occured on {_url}")

                try:
                    for result in comments["data"]["children"]:
                        async for item in kind(result):
                            yield (item)
                except GeneratorExit:
                    logging.info("[Reddit] Scraper generator exit...")
                    return
                except:
                    logging.exception(f"An error occured on {_url}")
    finally:
        await session.close()


def split_strings_subreddit_name(input_string):
    words = []
    start = 0

    for i in range(1, len(input_string)):
        if input_string[i].isupper():
            words.append(input_string[start:i])
            start = i

    words.append(input_string[start:])
    return ' '.join(words)


async def scrap_subreddit_new_layout(subreddit_url: str) -> AsyncGenerator[Item, None]:
    
    try:
        async with aiohttp.ClientSession() as session:
            url_to_fetch = subreddit_url
            logging.info("[Reddit] [NEW LAYOUT MODE] Opening: %s",url_to_fetch)
            async with session.get(url_to_fetch, 
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},     
                timeout=BASE_TIMEOUT) as response:
                html_content = await response.text()                    
                html_tree = fromstring(html_content)
                for post in html_tree.xpath("//shreddit-post/@permalink"):
                    url = post
                    if url.startswith("/r/"):
                        url = "https://www.reddit.com" + post
                    await asyncio.sleep(1)
                    try:
                        if "https" not in url:
                            url = f"https://reddit.com{url}"
                        async for item in scrap_post(url):
                            yield item
                    except Exception:
                        pass
    except:
        await session.close()

def find_permalinks(data):
    if isinstance(data, dict):
        if 'permalink' in data:
            yield data['permalink']
        for key, value in data.items():
            yield from find_permalinks(value)
    elif isinstance(data, list):
        for item in data:
            yield from find_permalinks(item)

async def scrap_subreddit_json(subreddit_url: str) -> AsyncGenerator[Item, None]:
    try:
        async with aiohttp.ClientSession() as session:
            url_to_fetch = subreddit_url + ".json"
            logging.info("[Reddit] [JSON MODE] opening: %s",url_to_fetch)
            async with session.get(url_to_fetch, 
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},     
                timeout=BASE_TIMEOUT) as response:
                # Parse JSON response
                data = await response.json()
                # Find all "permalink" values
                                
                permalinks = list(find_permalinks(data))

                for permalink in permalinks:
                    try:
                        if random.random() < SKIP_POST_PROBABILITY:
                            url = permalink
                            if "https" not in url:
                                url = f"https://reddit.com{url}"
                            async for item in scrap_post(url):
                                yield item
                    except Exception as e:
                        logging.exception(f"[Reddit] [JSON MODE] Error detected")

    except:
        await session.close()


DEFAULT_OLDNESS_SECONDS = 36000
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 5
DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS = 3
DEFAULT_LAYOUT_SCRAPING_WEIGHT = 0.05
DEFAULT_SKIP_PROBA = 0.1

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

        try:
            nb_subreddit_attempts = parameters.get(
                "nb_subreddit_attempts", DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS
            )
        except KeyError:
            nb_subreddit_attempts = DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS

        try:
            new_layout_scraping_weight = parameters.get(
                "new_layout_scraping_weight", DEFAULT_LAYOUT_SCRAPING_WEIGHT
            )
        except KeyError:
            new_layout_scraping_weight = DEFAULT_LAYOUT_SCRAPING_WEIGHT

        try:
            skip_post_probability = parameters.get(
                "skip_post_probability", DEFAULT_SKIP_PROBA
            )
        except KeyError:
            skip_post_probability = DEFAULT_SKIP_PROBA
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        nb_subreddit_attempts = DEFAULT_NUMBER_SUBREDDIT_ATTEMPTS
        new_layout_scraping_weight = DEFAULT_LAYOUT_SCRAPING_WEIGHT
        skip_post_probability = DEFAULT_SKIP_PROBA

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, nb_subreddit_attempts, new_layout_scraping_weight, skip_post_probability

def correct_reddit_url(url):
    parts = url.split("https://reddit.comhttps://", 1)
    if len(parts) == 2:
        corrected_url = "https://" + parts[1]
        return corrected_url
    return url

def post_process_item(item):    
    try:
        subreddit_name = extract_subreddit_name(item["url"])
        segmented_subreddit_strs = segment(subreddit_name)
        segmented_subreddit_name = " ".join(segmented_subreddit_strs)
        item["content"] = item["content"] + " . " + segmented_subreddit_name + " ."
    except Exception as e:
        logging.warning(f"[Reddit post_process_item] Word segmentation failed: {e}, ignoring...")
    try:
        item["url"] = correct_reddit_url(item["url"])
    except:
        logging.warning(f"[Reddit] failed to correct the URL of item %s",item["url"])
    return item

def is_valid_item(item, min_post_length):
    if len(item["content"])<min_post_length \
    or item["url"].startswith("https://reddit.comhttps:")  \
    or not ("reddit.com" in item["url"]) \
    or item["content"] == "[deleted]":
        return False
    else:
        return True

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    global MAX_EXPIRATION_SECONDS, SKIP_POST_PROBABILITY
    (
        max_oldness_seconds,
        MAXIMUM_ITEMS_TO_COLLECT,
        min_post_length,
        nb_subreddit_attempts,
        new_layout_scraping_weight,
        SKIP_POST_PROBABILITY
    ) = read_parameters(parameters)
    logging.info(f"[Reddit] Input parameters: {parameters}")
    MAX_EXPIRATION_SECONDS = max_oldness_seconds
    yielded_items = 0  # Counter for the number of yielded items

    for i in range(nb_subreddit_attempts):
        url = await generate_url(**parameters["url_parameters"])
        logging.info(f"[Reddit] Attempt {(i+1)}/{nb_subreddit_attempts} Scraping {url} with max oldness of {max_oldness_seconds}")
        if "reddit.com" not in url:
            raise ValueError(f"Not a Reddit URL {url}")
        url_parameters = url.split("reddit.com")[1].split("/")[1:]
        if "comments" in url_parameters:
            async for result in scrap_post(url):

                yielded_items += 1
                result = post_process_item(result)
                if is_valid_item(result, min_post_length):
                    logging.info(f"[REDDIT] Found Reddit post: {result}")
                    yield result
                if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                    break
        else:
            selected_function = scrap_subreddit_json
            if random.random() < new_layout_scraping_weight:
                selected_function = scrap_subreddit_new_layout
            async for result in selected_function(url):
                yielded_items += 1
                result = post_process_item(result)           
                if is_valid_item(result, min_post_length):
                    logging.info(f"[REDDIT] Found Reddit comment: {result}")
                    yield result
                if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                    break
