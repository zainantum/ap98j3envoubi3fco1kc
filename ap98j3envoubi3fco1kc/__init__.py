import random
import string
import uuid
import subprocess
import aiohttp
from aiohttp.client_exceptions import ClientError, ServerDisconnectedError, ClientHttpProxyError
from aiohttp_socks import ProxyConnector, SocksConnector
from stem import Signal
from stem.control import Controller
import dotenv
import os
import json
import gzip
import zlib
from io import BytesIO
import asyncio
import pycurl
from lxml import html
from typing import AsyncGenerator, List
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import time as tttime, datetime as datett, timedelta
from datetime import timezone
import pytz
import hashlib
import logging
from lxml.html import fromstring
from bs4 import BeautifulSoup
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
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Edge/129.0.2792.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/114.0.0.0',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1'
]

global MAX_EXPIRATION_SECONDS
global SKIP_POST_PROBABILITY
MAX_EXPIRATION_SECONDS = 80000
SKIP_POST_PROBABILITY = 0.1
BASE_TIMEOUT = 30
PROXIES_FILE = "proxies.json"

subreddits_top_225 = [
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/AITAH",
    "r/AITAH",
    "r/AITAH",
    "r/AmItheAsshole",
    "r/AmItheAsshole",
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
    "r/CryptoCurrency",
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
    "r/NoStupidQuestions",
    "r/NoStupidQuestions",
    "r/NoStupidQuestions",
    "r/passive_income",
    "r/pennystocks",
    "r/personalfinance",
    "r/PFtools",
    "r/politics",
    "r/politics",
    "r/politics",
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
    "r/BaldursGate3",
    "r/teenagers",
    "r/BaldursGate3",
    "r/teenagers",
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
    "r/india", "r/AskIndia", "r/bollywood", "r/Cricket", "r/Slovenia", "r/indiadiscussion",
    "r/germany", "r/de", "r/LearnGerman", "r/germusic",
    "r/france", "r/French", "r/paris", "r/europe", "r/relacionamentos",
    "r/japan", "r/japanlife", "r/newsokur", "r/learnjapanese",
    "r/brasil", "r/brasilivre", "r/riodejaneiro", "r/saopaulo",
    "r/mexico", "r/MexicoCity", "r/spanish", "r/yo_espanol",
    # 50 Most Popular News, Politics, and Finance/Economics Subreddits
    "r/news", "r/worldnews", "r/UpliftingNews", "r/nottheonion", "r/TrueReddit",
    "r/politics", "r/PoliticalDiscussion", "r/worldpolitics", "r/neutralpolitics", "r/Ask_Politics",
    "r/personalfinance", "r/investing", "r/StockMarket", "r/financialindependence", "r/economics",
    "r/TaylorSwift","r/TaylorSwift"
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


subreddits_top_1000 = [
    "r/AskReddit","r/AmItheAsshole","r/teenagers","r/NoStupidQuestions","r/BaldursGate3","r/facepalm","r/AITAH","r/TaylorSwift","r/soccer","r/WhitePeopleTwitter",
    "r/CryptoCurrency","r/FreeKarma4All","r/mildlyinfuriating","r/relationship_advice","r/politics","r/wallstreetbets","r/movies","r/gaming","r/UFOs","r/unpopularopinion",
    "r/PublicFreakout","r/antiwork","r/diablo4","r/amiugly","r/CFB","r/LiverpoolFC","r/nba","r/Damnthatsinteresting","r/AskUK","r/ask",
    "r/MonopolyGoTrading","r/nfl","r/therewasanattempt","r/SquaredCircle","r/worldnews","r/AskMen","r/memes","r/amiwrong","r/pcmasterrace","r/cats",
    "r/GachaClub","r/TrueOffMyChest","r/OnePiece","r/pathofexile","r/TikTokCringe","r/neoliberal","r/todayilearned","r/biggboss","r/Overwatch","r/coys",
    "r/TEMUcodeShare","r/news","r/KGBTR","r/FortNiteBR","r/golf","r/Genshin_Impact","r/deadbydaylight","r/leagueoflegends","r/Eldenring","r/baseball",
    "r/meirl","r/MadeMeSmile","r/ImTheMainCharacter","r/de","r/Philippines","r/Minecraft","r/Unexpected","r/HolUp","r/TwoXChromosomes","r/FantasyPL",
    "r/Serverlife","r/canada","r/MortalKombat","r/CrazyFuckingVideos","r/videogames","r/Christianity","r/NonCredibleDefense","r/redscarepod","r/shitposting","r/Karma4Free",
    "r/Market76","r/europe","r/DnD","r/remnantgame","r/pics","r/EscapefromTarkov","r/atheism","r/anime","r/futebol","r/namenerds",
    "r/conspiracy","r/weddingdress","r/explainlikeimfive","r/unitedkingdom","r/chelseafc","r/RoastMe","r/StupidFood","r/apexlegends","r/alevel","r/TooAfraidToAsk",
    "r/DestinyTheGame","r/BlackPeopleTwitter","r/Weird","r/Warthunder","r/shittytattoos","r/FaceRatings","r/TrueUnpopularOpinion","r/argentina","r/pokemon","r/OldSchoolCool",
    "r/television","r/Presidents","r/ufc","r/Starfield","r/AntiTrumpAlliance","r/CasualUK","r/JEENEETards","r/mildlyinteresting","r/PurplePillDebate","r/Canada_sub",
    "r/OnePiecePowerScaling","r/australia","r/2007scape","r/technology","r/tifu","r/newzealand","r/nrl","r/Destiny","r/Warframe","r/PoliticalCompassMemes",
    "r/PeterExplainsTheJoke","r/horror","r/Gunners","r/changemyview","r/Spiderman","r/barstoolsports","r/StreetFighter","r/totalwar","r/196","r/Teachers",
    "r/BestofRedditorUpdates","r/doordash","r/Parenting","r/ZeducationSubmissions","r/funny","r/dating_advice","r/BeAmazed","r/ireland","r/sex","r/Italia",
    "r/PokemonScarletViolet","r/AnarchyChess","r/motorcycles","r/AusFinance","r/reddevils","r/ChatGPT","r/Torontobluejays","r/Tinder","r/OUTFITS","r/SteamDeck",
    "r/h3h3productions","r/playstation","r/Brawlstars","r/whatisthisbug","r/sweden","r/gardening","r/WWE","r/harrypotter","r/MapPorn","r/LosAngeles",
    "r/dating","r/autism","r/Naruto","r/FunnyandSad","r/UkraineWarVideoReport","r/buildapc","r/Animemes","r/grandorder","r/indonesia","r/pcgaming",
    "r/Advice","r/ffxiv","r/marvelstudios","r/Youmo","r/relationships","r/FragReddit","r/Tekken","r/serbia","r/DunderMifflin","r/Mariners",
    "r/personalfinance","r/Romania","r/masterduel","r/polls","r/ThatsInsane","r/PremierLeague","r/startrek","r/Marriage","r/DeathBattleMatchups","r/EDH",
    "r/DotA2","r/RandomThoughts","r/BollyBlindsNGossip","r/LoveIslandUSA","r/whatcarshouldIbuy","r/Fauxmoi","r/fivenightsatfreddys","r/BravoRealHousewives","r/transformers","r/AskAnAmerican",
    "r/Genshin_Impact_Leaks","r/boxoffice","r/brasil","r/PersonalFinanceCanada","r/offmychest","r/NYYankees","r/BatmanArkham","r/DesignMyRoom","r/tennis","r/chile",
    "r/bleach","r/exmormon","r/travel","r/AmericaBad","r/tjournal_refugees","r/malelivingspace","r/orioles","r/tearsofthekingdom","r/aliens","r/mexico",
    "r/LivestreamFail","r/phillies","r/AskOldPeople","r/uknews","r/nursing","r/askgaybros","r/pettyrevenge","r/melbourne","r/trees","r/TwoBestFriendsPlay",
    "r/WTF","r/PokemonHome","r/Showerthoughts","r/MovieSuggestions","r/entertainment","r/AskMiddleEast","r/fut","r/StarWars","r/boston","r/MMA",
    "r/formula1","r/fantasyfootball","r/books","r/nextfuckinglevel","r/Doppleganger","r/hockey","r/LifeProTips","r/HomeImprovement","r/AussieTikTokSnark","r/batman",
    "r/Turkey","r/ukpolitics","r/Denmark","r/wow","r/MechanicAdvice","r/firstimpression","r/ValorantCompetitive","r/wholesomememes","r/Pikmin","r/conservativeterrorism",
    "r/army","r/careerguidance","r/delhi","r/Cooking","r/OriginalCharacter","r/NotHowGirlsWork","r/ADHD","r/vancouver","r/XboxSeriesX","r/confessions",
    "r/trans","r/FreeKarma4You","r/DarkAndDarker","r/adhdwomen","r/Grimdank","r/GenX","r/AMA","r/40kLore","r/IAmTheMainCharacter","r/geometrydash",
    "r/AEWOfficial","r/PokemonGoFriends","r/tipofmytongue","r/legaladvice","r/tf2","r/reddeadredemption","r/france","r/dankmemes","r/StardewValley","r/florida",
    "r/stopdrinking","r/gtaonline","r/NoFap","r/Braves","r/femalehairadvice","r/bjj","r/india","r/Costco","r/Suomi","r/PcBuild",
    "r/daddit","r/CasualPT","r/lookismcomic","r/Ningen","r/italy","r/adultingph","r/JoeRogan","r/whenthe","r/jobs","r/PS5",
    "r/fcbayern","r/IndianTeenagers","r/MemePiece","r/fightporn","r/DeepRockGalactic","r/BocaJuniors","r/selfie","r/Letterboxd","r/nhl","r/malaysia",
    "r/BabyBumps","r/DMZ","r/hungary","r/Conservative","r/skyrim","r/lego","r/football","r/vegan","r/90DayFiance","r/czech",
    "r/Polska","r/SFGiants","r/childfree","r/Piracy","r/Finanzen","r/DynastyFF","r/Truckers","r/tattooadvice","r/Hololive","r/PokemonTCG",
    "r/dndnext","r/thefighterandthekid","r/GunAccessoriesForSale","r/LeopardsAteMyFace","r/BigBrother","r/portugal","r/PoliticalHumor","r/Games","r/fo76","r/btd6",
    "r/exmuslim","r/terriblefacebookmemes","r/Watches","r/mlb","r/CombatFootage","r/starterpacks","r/AskArgentina","r/Patriots","r/shrooms","r/oddlyterrifying",
    "r/SubSimGPT2Interactive","r/Padres","r/FinalFantasy","r/ClashRoyale","r/runescape","r/Cricket","r/chicago","r/Dragonballsuper","r/rugbyunion","r/rolex",
    "r/Seattle","r/IndiaSpeaks","r/halo","r/oddlysatisfying","r/RealEstate","r/pokemongo","r/Rainbow6","r/travisscott","r/bloxfruits","r/Wrasslin",
    "r/magicTCG","r/Catholicism","r/Accounting","r/Sims4","r/thesopranos","r/Fantasy","r/HouseOfTheDragon","r/CATHELP","r/PokemonUnite","r/ontario",
    "r/HistoryMemes","r/maybemaybemaybe","r/Music","r/Austria","r/traaaaaaannnnnnnnnns2","r/Adulting","r/astrologymemes","r/saudiarabia","r/Terraria","r/Wellthatsucks",
    "r/nederlands","r/Tattoocoverups","r/Construction","r/unitedstatesofindia","r/work","r/UkrainianConflict","r/destiny2","r/KingOfTheHill","r/notinteresting","r/Chiraqology",
    "r/realmadrid","r/cscareerquestions","r/BattleBitRemastered","r/CuratedTumblr","r/ExplainTheJoke","r/desijo_b","r/weed","r/Justrolledintotheshop","r/VaushV","r/IdiotsInCars",
    "r/sanfrancisco","r/bayarea","r/AskAnAustralian","r/classicwow","r/askSingapore","r/NYStateOfMind","r/singularity","r/NASCAR","r/lotrmemes","r/AskACanadian",
    "r/KafkaMains","r/AirForce","r/Marvel","r/Austin","r/airsoft","r/ConeHeads","r/counting","r/rusAskReddit","r/greece","r/Piratefolk",
    "r/sysadmin","r/Breath_of_the_Wild","r/trashy","r/AskWomenOver30","r/mtg","r/Mommit","r/jerkofftoceleb","r/hearthstone","r/AskOuija","r/Frugal",
    "r/shittyfoodporn","r/CasualConversation","r/homeowners","r/cars","r/Ohio","r/rupaulsdragrace","r/awfuleverything","r/BrandNewSentence","r/RocketLeagueEsports","r/furry_irl",
    "r/femboy","r/lostarkgame","r/Andjustlikethat","r/baseballcards","r/80s","r/exjw","r/Astros","r/electricvehicles","r/WouldYouRather","r/KimetsuNoYaiba",
    "r/nbacirclejerk","r/popheads","r/TheDeprogram","r/loseit","r/lgbt","r/YuB","r/REBubble","r/toronto","r/memesopdidnotlike","r/RimWorld",
    "r/perth","r/OnePunchMan","r/comics","r/EnoughMuskSpam","r/pregnant","r/Warhammer40k","r/FFXVI","r/NameMyCat","r/moreplatesmoredates","r/ftm",
    "r/CleaningTips","r/TrueChristian","r/Dodgers","r/MobileLegendsGame","r/IndianGaming","r/chess","r/asoiaf","r/JRPG","r/IASIP","r/raisedbynarcissists",
    "r/PhotoshopRequest","r/AskNYC","r/seinfeld","r/AbruptChaos","r/Mortalkombatleaks","r/masseffect","r/Anticonsumption","r/SpidermanPS4","r/askhungary","r/angelsbaseball",
    "r/discordVideos","r/AskConservatives","r/london","r/mbti","r/inthenews","r/walmart","r/EASportsFC","r/talk_hunfluencers","r/Firearms","r/HuntShowdown",
    "r/GenZ","r/povertyfinance","r/UKPersonalFinance","r/kpopthoughts","r/MtF","r/KitchenConfidential","r/InstacartShoppers","r/Xennials","r/norge","r/minnesota",
    "r/AFL","r/Diablo","r/DIY","r/CarsIndia","r/actuallesbians","r/greentext","r/Undertale","r/maui","r/UnethicalLifeProTips","r/croatia",
    "r/VALORANT","r/mapporncirclejerk","r/BMW","r/TheOwlHouse","r/Ben10","r/delta","r/beauty","r/hiphopheads","r/LeagueOfMemes","r/RocketLeague",
    "r/yeezys","r/roblox","r/LateStageCapitalism","r/youngpeopleyoutube","r/MandJTV","r/ShuumatsuNoValkyrie","r/playboicarti","r/ukraine","r/recruitinghell","r/germany",
    "r/StableDiffusion","r/CallOfDutyMobile","r/kollywood","r/Gamingcirclejerk","r/Scotland","r/goodanimemes","r/Millennials","r/SmashBrosUltimate","r/Monopoly_GO","r/Boxing",
    "r/3Dprinting","r/boardgames","r/BITSPilani","r/Plumbing","r/tacticalgear","r/Barca","r/DemonSlayerAnime","r/discgolf","r/danganronpa","r/2american4you",
    "r/AskFrance","r/Bumble","r/electricians","r/PrequelMemes","r/FanFiction","r/thebachelor","r/MyHeroAcadamia","r/Boruto","r/90dayfianceuncensored","r/developersIndia",
    "r/handbags","r/AmITheDevil","r/cycling","r/Fallout","r/amcstock","r/ottawa","r/Quebec","r/Guildwars2","r/yugioh","r/UKJobs",
    "r/VeteransBenefits","r/Chainsawfolk","r/projectzomboid","r/whatisit","r/BrandonDE","r/Fishing","r/GregDoucette","r/truerateme","r/stocks","r/China_irl",
    "r/fromsoftware","r/texas","r/OffMyChestPH","r/formuladank","r/ClassicRock","r/ProgrammerHumor","r/AutismInWomen","r/starcitizen","r/singapore","r/Denver",
    "r/stunfisk","r/desabafos","r/ar15","r/BeelcitosMemes","r/dbz","r/Weddingattireapproval","r/Tools","r/lastimages","r/belowdeck","r/IndianDankMemes",
    "r/VerifiedFeet","r/MarvelStudiosSpoilers","r/ClashOfClans","r/meme","r/PhoenixSC","r/CasualPH","r/malehairadvice","r/TwoSentenceHorror","r/xqcow","r/NoRules",
    "r/PokemonSleep","r/Gundam","r/Residency","r/distressingmemes","r/BlueArchive","r/brasilivre","r/islam","r/asktransgender","r/Pathfinder2e","r/moviecritic",
    "r/SkincareAddiction","r/LoveIslandTV","r/UberEATS","r/WarhammerCompetitive","r/manga","r/antitrampo","r/Aquariums","r/Kenya","r/lanadelrey","r/fountainpens",
    "r/mariokart","r/nope","r/HVAC","r/chessbeginners","r/real_China_irl","r/Sephora","r/BG3Builds","r/gameofthrones","r/residentevil","r/Netherlands",
    "r/StudentLoans","r/AskALiberal","r/clevercomebacks","r/DokkanBattleCommunity","r/japanlife","r/AmazonFC","r/crochet","r/Kappachino","r/Sneakers","r/Hawaii",
    "r/sports","r/InstaCelebsGossip","r/Pandabuy","r/Eminem","r/farialimabets","r/MagicArena","r/pittsburgh","r/blankies","r/TheSilphRoad","r/nova",
    "r/lawncare","r/TeenMomOGandTeenMom2","r/EngagementRings","r/debbiethepetladysnark","r/iphone","r/HilariaBaldwin","r/poker","r/flying","r/auckland","r/Scams",
    "r/JoeyBdezSnark2","r/sportsbook","r/TroChuyenLinhTinh","r/technicallythetruth","r/evilautism","r/SeattleWA","r/Colombia","r/AnimalCrossing","r/dataisbeautiful","r/Bitcoin",
    "r/GlobalOffensive","r/Deltarune","r/footballmanagergames","r/whowouldwin","r/punk","r/starwarsmemes","r/NewParents","r/thenetherlands","r/Louisville","r/aww",
    "r/canadahousing","r/overwatch2","r/AustralianPolitics","r/ContagiousLaughter","r/KidsAreFuckingStupid","r/FifaCareers","r/TeslaModel3","r/lonely","r/MeJulgue","r/CrusaderKings",
    "r/Kanye","r/OutsideLands","r/bald","r/LegalAdviceUK","r/DragonballLegends","r/ScottishFootball","r/USPS","r/whatsthisplant","r/USMC","r/crossdressing",
    "r/howardstern","r/CitiesSkylines","r/coolguides","r/InfluencergossipDK","r/StellarCannaCoin","r/RoastMyCar","r/zelda","r/neopets","r/Berserk","r/DiWHY",
    "r/NoMansSkyTheGame","r/BBBY","r/TheSimpsons","r/FUTMobile","r/NBA2k","r/ChoosingBeggars","r/wichsbros_DEU2023","r/AmITheAngel","r/whatsthisbug","r/fantanoforever",
    "r/houston","r/Guitar","r/Mario","r/datingoverforty","r/WorkReform","r/FashionReps","r/SWGalaxyOfHeroes","r/pokemontrades","r/femboymemes","r/DC_Cinematic",
    "r/HighStrangeness","r/RedditPregunta","r/Bolehland","r/beards","r/Random_Acts_Of_Amazon","r/LSD","r/worldbuilding","r/dccomicscirclejerk","r/MySingingMonsters","r/Persona5",
    "r/geography","r/woodworking","r/southpark","r/NewTubers","r/BruceDropEmOff","r/videos","r/Whatcouldgowrong","r/realhousewives","r/TrueCrimeDiscussion","r/skyrimmods",
    "r/redditmoment","r/Columbus","r/nvidia","r/washingtondc","r/Competitiveoverwatch","r/KerbalSpaceProgram","r/Transformemes","r/houseplants","r/collapse","r/CroatiaAlt",
    "r/blackdesertonline","r/antinatalism","r/Teenager_Polls","r/northernireland","r/martialarts","r/JustUnsubbed","r/saltierthankrayt","r/ChainsawMan","r/Economics","r/PathOfExileBuilds",
    "r/steak","r/KUWTKsnark","r/eu4","r/Broadway","r/HypixelSkyblock","r/denvernuggets","r/fantasybaseball","r/CPTSD","r/NFA","r/comicbookmovies",
    "r/ksi","r/Norway","r/PetiteFashionAdvice","r/nottheonion","r/Guiltygear","r/science","r/interestingasfuck","r/virtualreality","r/rareinsults","r/bangalore",
    "r/TIKTOKSNARKANDGOSSIP","r/yeat_","r/whatisthiscar","r/DCcomics","r/nyc","r/ShittyMapPorn","r/Fighters","r/insanepeoplefacebook","r/HousingUK","r/BattleForDreamIsland",
    "r/forhonor","r/Stellaris","r/MakeupAddiction","r/phish","r/askTO","r/MaliciousCompliance","r/battlecats","r/gamingsuggestions","r/BisexualTeens","r/NonPoliticalTwitter",
    "r/AppleWatch","r/Qult_Headquarters","r/armoredcore","r/thewalkingdead","r/SCJerk","r/HotWheels","r/yakuzagames","r/xmen","r/CatAdvice","r/CUETards",
    "r/Winnipeg","r/StardustCrusaders","r/trippieredd","r/space","r/halifax","r/TattooDesigns","r/EntitledPeople","r/WatchPeopleDieInside","r/conspiracy_commons","r/ToiletPaperUSA",
    "r/UPSers","r/ARK","r/ThePPShow","r/Sub4Sub","r/SonicTheHedgehog","r/RepTime","r/EuSouOBabaca","r/IndianBoysOnTinder","r/CasualRO","r/bindingofisaac",
    "r/4chan","r/GunMemes","r/coins","r/tressless","r/csgo","r/Audi","r/Nanny","r/tacobell","r/ironscape","r/orangecounty",
    "r/TerrifyingAsFuck","r/Edmonton","r/ManchesterUnited","r/Ultrakill","r/Amberverse_","r/MensRights","r/AmazonDSPDrivers","r/preppers","r/dubai","r/valheim",
    "r/MinecraftMemes","r/bigdickproblems","r/Funnymemes","r/Pikabu","r/okbuddychicanery","r/egg_irl","r/Ticos","r/FirstTimeHomeBuyer","r/NatureIsFuckingLit","r/SelfieOver25",
    "r/wildrift","r/Smite","r/30PlusSkinCare","r/Abortiondebate","r/AzureLane","r/Memes_Of_The_Dank","r/PokemonRoleplays","r/xbox","r/IThinkYouShouldLeave","r/WaltDisneyWorld",
    "r/UrbanHell","r/liseliler","r/Miata","r/CarTalkUK","r/selfimprovement","r/GooglePixel","r/RaidShadowLegends","r/Genshin_Memepact","r/RomanceBooks","r/lyftdrivers",
    "r/Watchexchange","r/vexillology","r/BreakUps","r/newjersey","r/FreeCompliments","r/brisbane","r/PoliticalCompass","r/Amd","r/CODZombies","r/DevilMayCry",
    "r/AskMechanics","r/Vanderpumpaholics","r/AdorableNudes","r/NoJumper","r/Totaldrama","r/mylittlepony","r/ffxivdiscussion","r/bisexual","r/indiadiscussion","r/kingcobrajfs",
    "r/MTB","r/WFH","r/snappijuorut","r/Dallas","r/AO3","r/breakingbad","r/ufo","r/DebateReligion","r/PoliticalMemes","r/Gunpla",
    "r/Rabbits","r/Slovenia","r/ich_iel","r/splatoon","r/BanPitBulls","r/suggestmeabook","r/FLMedicalTrees","r/relacionamentos","r/FireEmblemHeroes","r/GoodAssSub",
    "r/HFY","r/19684","r/RobloxAvatars","r/whatisthisthing","r/OtomeIsekai","r/Kengan_Ashura","r/JUSTNOMIL","r/USCIS","r/homelab","r/gundeals",
    "r/doctorsUK","r/Entrepreneur","r/bluey","r/careeradvice","r/kolkata","r/arborists","r/TheMajorityReport","r/4Runner","r/GalaxyFold","r/gaybros",
    "r/Calgary","r/furry","r/csMajors","r/Bedbugs","r/DBZDokkanBattle","r/mumbai","r/popheadscirclejerk","r/marvelmemes","r/Egypt","r/Topster",
]

tor_lock = asyncio.Lock()
# A flag to track whether the Tor circuit rotation is already in progress
circuit_rotation_in_progress = False
TOR_PORTS = [9050, 9052, 9054, 9056, 9058, 9060, 9062, 9064, 9066, 9068, 9070, 9072, 9074, 9076, 9078, 9080, 9082, 9084, 9086, 9088]
TOR_PORTS = [9050, 9052, 9054, 9056, 9058, 9060, 9062, 9064, 9066, 9068, 9070, 9072, 9074, 9076, 9078, 9080, 9082, 9084, 9086, 9088, 9090, 9092, 9094, 9096, 9098, 9100, 9102, 9104, 9106, 9108, 9110, 9112, 9114, 9116, 9118, 9120, 9122, 9124, 9126, 9128]
list_sub = [
    "ar.reddit.com",
    "zh.reddit.com",
    "zh-hant.reddit.com",
    "cs.reddit.com",
    "da.reddit.com",
    "nl.reddit.com",
    "en-gb.reddit.com",
    "fi.reddit.com",
    "fr.reddit.com",
    "de.reddit.com",
    "el.reddit.com",
    "he.reddit.com",
    "hu.reddit.com",
    "is.reddit.com",
    "id.reddit.com",
    "it.reddit.com",
    "ja.reddit.com",
    "ko.reddit.com",
    "no.reddit.com",
    "pl.reddit.com",
    "pt-br.reddit.com",
    "pt.reddit.com",
    "ro.reddit.com",
    "ru.reddit.com",
    "es.reddit.com",
    "sv.reddit.com",
    "tr.reddit.com",
    "old.reddit.com",
    "np.reddit.com",
    "m.reddit.com"
]


async def generate_random_string():
    numeric_id = ''.join(random.choices(string.digits, k=15))
    timestamp = datett.now() - timedelta(days=random.randint(0, 30))
    timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    random_hash = uuid.uuid4().hex[:40]
    random_string = f"{numeric_id}%2C{timestamp_str}%2C{random_hash}"
    return random_string

async def load_env_variable(key, default_value=None, none_allowed=False):
    v = os.getenv(key, default=default_value)
    if v is None and not none_allowed:
        raise RuntimeError(f"{key} returned {v} but this is not allowed!")
    return v


async def get_email(env):
    dotenv.load_dotenv(env, verbose=True)
    now_utc = datett.now(pytz.utc).time()
    start_time = tttime(0, 0)
    end_time = tttime(12, 0)
    # if start_time <= now_utc < end_time:
    #     default_var = await load_env_variable("SCWEET_USERNAME", none_allowed=True)
    #     if len(default_var) < 70:
    #         default_var = await load_env_variable("SCWEET_EMAIL", none_allowed=True)
    # else:
    #     default_var = await load_env_variable("SCWEET_EMAIL", none_allowed=True)
    
    return await generate_random_string()


async def get_token(env):
    dotenv.load_dotenv(env, verbose=True)
    default_var = await load_env_variable("SCWEET_USERNAME", none_allowed=True)
    return default_var


async def set_session_cookies(session):
    reddit_session_cookie = await get_email(".env")
    # #cookie = aiohttp.CookieJar()        
    # cookie_jar.update_cookies({'reddit_session': reddit_session_cookie}, response_url='https://www.reddit.com')
    session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
    # cookie.update_cookies({'reddit_session': reddit_session_cookie}, response_url=response_url)
    # session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
    # cookie_jar = aiohttp.CookieJar()
    # cookie_jar.update_cookies({'reddit_session': reddit_session_cookie}, response_url='https://www.reddit.com')
    # session.cookie_jar.update_cookies({
    #         'reddit_session': reddit_session_cookie
    #     }, response_url='https://www.reddit.com')
    logger.info("[Reddit] Session cookies updated")

# async def fetch_proxies(session, url):
#     async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
#         logging.info(f"Response retrieve proxies: {response.status}")
#         if response.status == 200:
#             content = await response.text()
#             tree = html.fromstring(content)
#             proxies = []
#             rows = tree.xpath('/html/body/section[1]/div/div[2]/div/table/tbody/tr')

#             tasks = []
#             for row in rows:
#                 last_checked_text = row.xpath('.//td[8]/text()')[0]
#                 logging.info(f"Proxies last checked: {last_checked_text}")
#                 if not "hour" in last_checked_text:
#                     ip = row.xpath('.//td[1]/text()')[0]
#                     port = row.xpath('.//td[2]/text()')[0]
#                     protocol = "https" if "yes" in row.xpath('.//td[7]/text()')[0].lower() else "http"
#                     proxy = f"{protocol}://{ip}:{port}"
#                     if "https" in proxy:
#                         tasks.append(test_and_append_proxy(session, proxy, "https://reddit.com", proxies))
#                     else:
#                         tasks.append(test_and_append_proxy(session, proxy, "http://reddit.com", proxies))

#             await asyncio.gather(*tasks)
#             return proxies
#         else:
#             logging.info(f"Failed to retrieve proxies: {response.status}")
#             return []

# async def fetch_proxies_from_api(session, url):
#     try:
#         async with session.get(url) as response:
#             if response.status == 200:
#                 content_type = response.headers.get('Content-Type', '')
#                 content = await response.text()
#                 proxies = []
                
#                 if 'application/json' in content_type:
#                     data = await response.json()
#                     for proxy in data.get('data', []):
#                         ip = proxy.get('ip')
#                         port = proxy.get('port')
#                         protocols = proxy.get('protocols', [])
#                         if 'http' in protocols or 'https' in protocols:
#                             protocol = 'https' if 'https' in protocols else 'http'
#                             proxies.append(f"{protocol}://{ip}:{port}")
#                 else:
#                     lines = content.splitlines()
#                     for line in lines:
#                         if line.startswith("http://") or line.startswith("https://"):
#                             proxies.append(line.strip())
#                         else:
#                             proxies.append(f"http://{line.strip()}")
                
#                 logging.info(f"Fetched {len(proxies)} proxies from {url}")
#                 return proxies
#             else:
#                 logging.error(f"Failed to fetch proxies from {url}, status code: {response.status}")
#                 return []
#     except aiohttp.ClientError as e:
#         logging.error(f"ClientError while fetching proxies from {url}: {e}")
#         return []

# Fetch proxies from spys.one
async def fetch_proxies_spys_one(session, url):
    async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
        logging.info(f"Response retrieve proxies from spys.one: {response.status}")
        if response.status == 200:
            content = await response.text()
            tree = html.fromstring(content)
            proxies = []
            
            rows = tree.xpath("//tr[contains(@class, 'spy1xx') or contains(@class, 'spy1x')]")
            for row in rows:
                if len(row.xpath('.//td')) > 6:
                    ip = row.xpath('.//td[1]//text()')[0]
                    proxy = f"http://{ip}"
                    print("proxies spys one ", proxy)
                    proxies.append(proxy)

            logging.info(f"Fetched {len(proxies)} proxies from {url}")
            return proxies
        else:
            logging.error(f"Failed to retrieve proxies from spys.one: {response.status}")
            return []

# Function to extract port from the script element
def extract_port_from_script(script_text):
    pattern = re.compile(r"(?<=\+)\d+")
    port_parts = pattern.findall(script_text)
    return ''.join(port_parts)

# Fetch proxies from HTML-based URLs
async def fetch_proxies(session, url):
    async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
        logging.info(f"Response default retrieve proxies: {response.status}")
        if response.status == 200:
            content = await response.text()
            tree = html.fromstring(content)
            proxies = []
            rows = tree.xpath('/html/body/section[1]/div/div[2]/div/table/tbody/tr')

            for row in rows:
                last_checked_text = row.xpath('.//td[8]/text()')[0]
                # logging.info(f"Proxies last checked: {last_checked_text}")
                if not "hour" in last_checked_text:
                    ip = row.xpath('.//td[1]/text()')[0]
                    port = row.xpath('.//td[2]/text()')[0]
                    protocol = "https" if "yes" in row.xpath('.//td[7]/text()')[0].lower() else "http"
                    proxy = f"{protocol}://{ip}:{port}"
                    
                    proxies.append(proxy)

            logging.info(f"Fetched {len(proxies)} proxies from {url}")
            return proxies
        else:
            logging.info(f"Failed to retrieve proxies: {response.status}")
            return []

async def fetch_proxies_nova(session, url):
    async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
        logging.info(f"Response retrieve nova proxies: {response.status}")
        if response.status == 200:
            content = await response.text()
            tree = html.fromstring(content)
            proxies = []
            rows = tree.xpath('//*[@id="tbl_proxy_list"]/tbody/tr')

            for row in rows:
                ip = row.xpath('.//td[1]/text()')[0]
                port = row.xpath('.//td[2]/text()')[0]
                proxy = f"http://{ip}:{port}"
                proxies.append(proxy)

            logging.info(f"Fetched {len(proxies)} proxies from {url}")
            return proxies
        else:
            logging.info(f"Failed to retrieve proxies: {response.status}")
            return []

# Fetch proxies from JSON-based APIs
async def fetch_proxies_from_api(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '')
                content = await response.text()
                proxies = []
                
                if 'application/json' in content_type:
                    data = await response.json()
                    if "lumiproxy" in url:
                        data = data["data"]
                        for proxy in data.get('list', []):
                            ip = proxy.get('ip')
                            port = proxy.get('port')
                            proxy = f"http://{ip}:{port}"
                            proxies.append(proxy)
                    else:
                        for proxy in data.get('data', []):
                            ip = proxy.get('ip')
                            port = proxy.get('port')
                            protocols = proxy.get('protocols', [])
                            if 'http' or 'https' in protocols:
                                protocol = 'https' if 'https' in protocols else 'http'
                                proxy = f"{protocol}://{ip}:{port}"
                                proxies.append(proxy)
                else:
                    lines = content.splitlines()
                    for line in lines:
                        if line.startswith("http://") or line.startswith("https://"):
                            proxy = line.strip()
                        else:
                            proxy = f"http://{line.strip()}"
                        
                        proxies.append(proxy)
                
                logging.info(f"Fetched {len(proxies)} proxies from {url}")
                return proxies
            else:
                logging.error(f"Failed to fetch proxies from {url}, status code: {response.status}")
                return []
    except aiohttp.ClientError as e:
        logging.error(f"ClientError while fetching proxies from {url}: {e}")
        return []

async def fetch_proxies_from_freeproxyworld(session):
    url = "https://www.freeproxy.world/"
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        proxies = []
        rows = soup.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) > 1:
                ip = columns[0].text.strip()
                port = columns[1].text.strip()
                protocol = columns[2].text.strip().lower()
                if protocol in ['http', 'https']:
                    proxies.append(f"{protocol}://{ip}:{port}")
        return proxies

async def fetch_proxies_from_free_proxy_cz(session):
    url = "http://free-proxy.cz/en/"
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        proxies = []
        rows = soup.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) > 1:
                ip_port = columns[0].text.strip()
                protocol = columns[1].text.strip().lower()
                if protocol in ['http', 'https']:
                    proxies.append(f"{protocol}://{ip_port}")
        return proxies

def generate_ptools_urls(base_url, total_pages):
    return [base_url.format(page) for page in range(1, total_pages + 1)]

async def fetch_proxies_ptools(session, url):
    async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
        logging.info(f"Response default retrieve proxies: {response.status}")
        if response.status == 200:
            content = await response.text()
            tree = html.fromstring(content)
            proxies = []
            rows = tree.xpath('//*[@id="ct-main"]/main/table/tbody/tr')
            proxies = []
            for row in rows:
                if "http" in row.xpath('.//td[3]/text()')[0].lower():
                    ip = row.xpath('.//td[1]/text()')[0]
                    proxy = f"http://{ip}:80"
                    proxies.append(proxy)

            logging.info(f"Fetched {len(proxies)} proxies from {url}")
            return proxies
        else:
            logging.info(f"Failed to retrieve proxies: {response.status}")
            return []

# Main function to get all proxies
async def get_proxy():
    html_urls = [
        "https://www.sslproxies.org/",
        "https://www.us-proxy.org/",
        "https://free-proxy-list.net/",
        "https://free-proxy-list.net/anonymous-proxy.html",
        "https://free-proxy-list.net/uk-proxy.html",
        "https://spys.one/proxy-port/8080/",
        "https://spys.one/proxy-port/80/",
        "https://spys.one/proxy-port/3128/",
        "https://spys.one/proxy-port/8080/",
        "https://spys.one/proxy-port/999/",
        "https://spys.one/proxy-port/1080/"
    ]

    api_urls = [
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&proxy_format=protocolipport&format=text",
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
        "https://api.lumiproxy.com/web_v1/free-proxy/list?page_size=1500&page=1&language=en-us"
    ]

    nova_urls = [
        "https://www.proxynova.com/proxy-server-list/country-gb",
        "https://www.proxynova.com/proxy-server-list/country-us",
        "https://www.proxynova.com/proxy-server-list/country-de",
        "https://www.proxynova.com/proxy-server-list/country-ar",
        "https://www.proxynova.com/proxy-server-list/country-br",
        "https://www.proxynova.com/proxy-server-list/country-fi",
        "https://www.proxynova.com/proxy-server-list/country-fr",
        "https://www.proxynova.com/proxy-server-list/country-in",
        "https://www.proxynova.com/proxy-server-list/country-kr",
        "https://www.proxynova.com/proxy-server-list/country-ru"
    ]

    # proxy_tools = [
    #     "https://proxy-tools.com/proxy?page=1",
    #     "https://proxy-tools.com/proxy?page=2",
    #     "https://proxy-tools.com/proxy?page=3",
    #     "https://proxy-tools.com/proxy?page=4",
    #     "https://proxy-tools.com/proxy?page=5",
    #     "https://proxy-tools.com/proxy?page=6",
    #     "https://proxy-tools.com/proxy?page=7",
    #     "https://proxy-tools.com/proxy?page=8",
    #     "https://proxy-tools.com/proxy?page=9",
    #     "https://proxy-tools.com/proxy?page=10"
    # ]
    
    async with aiohttp.ClientSession() as session:
        # Fetch proxies from HTML-based URLs
        # tasks_html = [fetch_proxies(session, url) if 'spys.one' not in url else fetch_proxies_spys_one(session, url) for url in html_urls]
        # results_html = await asyncio.gather(*tasks_html)
        
        # Fetch proxies from JSON-based APIs
        # tasks_api = [fetch_proxies_from_api(session, url) for url in api_urls]
        # results_api = await asyncio.gather(*tasks_api)

        # tasks_ptools = [fetch_proxies_ptools(session, url) for url in proxy_tools]
        page_urls = generate_ptools_urls("https://proxy-tools.com/proxy/https?page={}", 15) + generate_ptools_urls("https://proxy-tools.com/proxy/http?page={}", 15) + generate_ptools_urls("https://proxy-tools.com/proxy/socks?page={}", 15) + generate_ptools_urls("https://proxy-tools.com/proxy/anonymous?page={}", 15)
        tasks_ptools = [fetch_proxies_ptools(session, url) for url in page_urls]
        results_ptools = await asyncio.gather(*tasks_ptools)

        # Fetch proxies from Nova
        # tasks_nova = [fetch_proxies_nova(session, url) for url in nova_urls]
        # results_nova = await asyncio.gather(*tasks_nova)

        # Fetch proxies from other sources
        # tasks_other = [
        #     fetch_proxies_from_freeproxyworld(session),
        #     fetch_proxies_from_free_proxy_cz(session),
        # ]
        # results_other = await asyncio.gather(*tasks_other)
        
        # Combine all results
        all_proxies = []
        for proxy_list in results_ptools:
            all_proxies.extend(proxy_list)
        
        # Remove duplicates
        unique_proxies = list(set(all_proxies))
        
        # Test and append valid proxies
        logging.info(f"Total proxies to test: {len(unique_proxies)}")
        valid_proxies = []
        tasks = []
        for proxy in unique_proxies:
            test_url = "https://reddit.com" if "https" in proxy else "http://reddit.com"
            task = asyncio.create_task(test_and_append_proxy(session, proxy, test_url, valid_proxies))
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        if valid_proxies:
            logging.info(f"Total valid proxies: {len(valid_proxies)}")
            return valid_proxies
        else:
            return None

async def test_and_append_proxy(session, proxy, test_url, proxies):
    if "socks4://" in proxy:
        proxy = proxy.replace("socks4://","")
    is_proxy_valid = await test_proxy(session, proxy, test_url)
    if not is_proxy_valid and "https" in proxy:
        # logging.warning(f"HTTPS failed, trying HTTP: {proxy}")
        is_proxy_valid = await test_proxy(session, proxy.replace("https", "http"), "http://reddit.com")
        if is_proxy_valid:
            logging.warning(f"Found valid proxy (HTTP): {proxy.replace('https','http')}")
            proxies.append(proxy.replace("https", "http"))
    elif is_proxy_valid:
        logging.warning(f"Found valid proxy: {proxy}")
        proxies.append(proxy)

async def test_proxy_curl(proxy, test_url):
    logging.warning(f"Try proxy using curl: {proxy}")
    try:
        curl_command = [
            'curl',
            '-L',  
            '-x', proxy, 
            '--max-time', '5',  
            '-o', '/dev/null', 
            '-s', 
            '-w', '%{http_code}', 
            test_url
        ]

        result = subprocess.run(curl_command, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip() == "200":
            return True
    except Exception as e:
        return False

async def test_proxy_pycurl(proxy, test_url):
    logging.warning(f"Try proxy using pycurl: {proxy}")
    buffer = ()
    c = pycurl.Curl()
    
    try:
        c.setopt(c.URL, test_url)
        c.setopt(c.PROXY, proxy)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.TIMEOUT, 5)
        c.setopt(c.NOBODY, True)
        c.perform()
        
        response_code = c.getinfo(pycurl.RESPONSE_CODE)
        if response_code == 200:
            return True
        else:
            return False
    except pycurl.error as e:
        logging.error(f"pycURL error for {test_url} with proxy {proxy}: {str(e)}")
        return False
    finally:
        c.close()

async def test_proxy(session, proxy, test_url):
    try:
        async with session.get(test_url, proxy=proxy, timeout=30) as response:
            if response.status == 200:
                return True
            # else:
            #     return await test_proxy_curl(proxy, test_url)
    except Exception as e:
        return False

def load_proxies():
    if os.path.exists(PROXIES_FILE):
        with open(PROXIES_FILE, "r") as file:
            data = json.load(file)
            timestamp = datett.fromisoformat(data["timestamp"])
            proxies = data["proxies"]
            sources = data["sources"]
            return sources, proxies
    return None, None

async def load_proxies_git():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://raw.githubusercontent.com/zainantum/mantle-nft-watcher/main/proxies.json") as response:
                if response.status == 200:
                    content = await response.text()
                    proxies = json.loads(content)
                    save_proxies(proxies, "git")
                    return proxies
                else:
                    logging.error(f"Failed to load proxies from GitHub. Status code: {response.status}")
                    return []
    except Exception as e:
        logging.error(f"Error loading proxies: {e}")
        return []

def remove_proxy_from_list(proxy, proxies):
    return [p for p in proxies if p != proxy]

def remove_proxies(proxy):
    if os.path.exists(PROXIES_FILE):
        with open(PROXIES_FILE, "r") as file:
            data = json.load(file)
            proxies = data["proxies"]
            proxies = remove_proxy_from_list(proxy, proxies)
            save_proxies(proxies, data["sources"])

def save_proxies(proxies, source):
    unique_proxies = list(set(proxies))
    
    data = {
        "timestamp": datett.now().isoformat(),
        "proxies": unique_proxies,
        "sources": source
    }
    
    with open(PROXIES_FILE, "w") as file:
        json.dump(data, file, indent=4)
        
    logging.info(f"Saved proxies. Total unique proxies: {len(unique_proxies)}")


async def rotate_tor_circuit(controller_port):
    try:
        with Controller.from_port(port=controller_port) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            print("Tor circuit rotated successfully!")

        # Simulate a delay after rotating the circuit (e.g., to wait for the circuit to be ready)
        logging.info(f"Waiting for the new circuit to stabilize after rotating tor {controller_port}...")
        await asyncio.sleep(5)

    except Exception as e:
        logging.error(f"Error rotating Tor circuit: {e}")


async def manage_proxies():
    sources, proxies = load_proxies()
    if not proxies:
        logging.info("Fetch on github...")
        proxies = await load_proxies_git()
        if not proxies:
            logging.info("No proxies left, fetching new proxies...")
            proxies = await get_proxy()
            save_proxies(proxies, "git")
        # if sources == "git":
        #     logging.info("No proxies left, fetching new proxies...")
        #     proxies = await get_proxy()
        #     save_proxies(proxies, "scrape")
        # else:
        #     logging.info("Fetch on github...")
        #     proxies = await load_proxies_git()
        #     if not proxies:
        #         logging.info("No proxies left, fetching new proxies...")
        #         proxies = await get_proxy()
        #         save_proxies(proxies, "scrape")
        
    return random.choice(proxies) if proxies else None

# async def manage_proxies():
#     timestamp, proxies = load_proxies()
#     if not timestamp or (datett.now() - timestamp > timedelta(minutes=15)):
#         logging.info("Fetching new proxies...")
#         proxies = await get_proxy()
#         save_proxies(proxies)
#     else:
#         if proxies:
#             logging.info("Using existing proxies from JSON file.")
#         else:
#             logging.info("Fetching new proxies...")
#             proxies = await get_proxy()
#             save_proxies(proxies)
    
#     return random.choice(proxies)

async def fetch_with_tor_socks5h(url: str, user_agent: str, socks_port: str) -> dict:
    """Fetch the URL using curl with a socks5h proxy (DNS resolution via Tor)."""
    try:
        # Choose a random Tor port from the available options
        if not socks_port:
            socks_port = random.choice(TOR_PORTS)
            
        tor_proxy = f"socks5h://127.0.0.1:{socks_port}"
        
        # Create the curl command with the socks5h proxy to use DNS resolution through Tor
        curl_command = [
            "curl",
            "-x", tor_proxy,
            "-s",  # Silent mode (no progress output)
            "-L",  # Follow redirects
            "-m", "7",  # Set the timeout for the request
            url
        ]

        # command = [
        #     "curl", "-L", "-s",  # -i includes headers, -s is silent (no progress bar)
        #     "-x", proxy,         # Proxy
        #     "--max-time", "15",  # Timeout after 30 seconds
        #     url_to_fetch         # URL to fetch
        # ]
        
        # Execute the curl command and capture the output
        logging.warning(f"[Tor] Rate limit encountered for {url}, retrying with curl socks5h {tor_proxy}...")
        result = subprocess.run(curl_command, capture_output=True, text=True)

        # Check if curl ran successfully
        if result.returncode != 0:
            logging.error(f"[Tor] curl command failed with exit code {result.returncode}")
            return {}

        if '<html>' in result.stdout.lower():
            if 'too many requests' in result.stdout.lower():
                logging.warning(f"[Tor] Rate limiting detected (HTTP 429). URL: {url} with curl socks5h {tor_proxy}")
            else:
                logging.warning(f"[Tor] Unexpected HTML response received: {result.stdout[:500]}")
            return {}

        # Attempt to parse the result as JSON
        try:
            logging.info(f"CURL socks5h success with proxy {tor_proxy}")
            response_data = json.loads(result.stdout)
            return response_data
        except json.JSONDecodeError:
            logging.error(f"[Tor] Failed to decode JSON from curl socks5h output: {result.stdout[:500]}")
            return {}

    except Exception as e:
        logging.error(f"[Tor] Error during curl fetch: {e}")
        return {}

async def get_tor_session(proxy_type: str, socks_port: str) -> aiohttp.ClientSession:
    """Return a new aiohttp session configured to use Tor with either socks5 or socks5h."""
    
    # Validate proxy_type
    if proxy_type not in ["socks5", "socks5h"]:
        raise ValueError("proxy_type must be either 'socks5' or 'socks5h'")

    if not socks_port:
        socks_port = random.choice(TOR_PORTS)
        
    tor_proxy = f"{proxy_type}://127.0.0.1:{socks_port}"
    logging.info(f"[Tor] Fetching with proxy {tor_proxy}")
    if proxy_type == "socks5":
        connector = SocksConnector.from_url(tor_proxy)
    else:
        connector = ProxyConnector.from_url(tor_proxy)
        
    session = aiohttp.ClientSession(connector=connector)
    return session


async def fetch_with_tor(url: str, user_agent: str, proxy_type: str, socks_port: str) -> dict:
    """Fetch the URL through Tor, retrying in case of rate limiting or errors."""
    try:
        async with await get_tor_session(proxy_type, socks_port) as session:
            logging.info(f"[Tor] Fetching {url} with Tor")
            async with session.get(url, headers={"User-Agent": user_agent}, timeout=BASE_TIMEOUT) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/json' in content_type:
                        return await response.json()
                    else:
                        logging.warning(f"[Tor] Unexpected content type: {content_type}")
                        return {}
                elif response.status == 429 or response.status == 403:
                    logging.warning(f"[Tor] Rate limit encountered for {url}, return nothing...")
                    return await fetch_subreddit_json_using_sub_domain_and_curl(url)
                    # if "reddit.com" in url:
                    #     url = url.replace("reddit.com","reddittorjg6rue252oqsxryoxengawnmo46qy4kyii5wtqnwfj4ooad.onion")
                    #     if not "www." in url and not "comment" in url:
                    #         url = url.replace("reddittor", "www.reddittor")
                    #     return await fetch_with_tor_socks5h(url, user_agent, socks_port)
                    #     #return await fetch_with_tor(url, user_agent, "socks5h")
                    # else:
                    #     logging.warning(f"[Tor] Rate limit encountered for {url}, return nothing...")
                    #     return {}
                else:
                    logging.warning(f"[Tor] Error fetching {url} with status: {response.status}")
                    return {}
    except Exception as e:
        logging.warning(f"[Tor] Error: {e}")
        return {}

async def find_random_subreddit_for_keyword_using_sub_domain(session: aiohttp.ClientSession, keyword: str) -> dict:
    url_to_fetch = keyword
    logging.info("[Reddit] opening: %s", url_to_fetch)
    reddit_session_cookie = await get_email(".env") 
    cookies = {'reddit_session': reddit_session_cookie}
    session.cookie_jar.update_cookies(cookies)
    async with session.get(url_to_fetch, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
        if response.status == 429:
            logging.warning("[Reddit] Cannot search keyword because rate limited even using subdomain for %s.", url_to_fetch)
            return ''
            
        if response.status != 200:
            logging.error(f"[Reddit] Non-200 status code for keyword: {response.status} for {url_to_fetch}")
            return ''
            
        html_content = await response.text()
        tree = html.fromstring(html_content)
        urls = [
            url
            for url in tree.xpath('//a[contains(@href, "/r/")]//@href')
            if not "/r/popular" in url
        ]
        
        if not urls:
            logging.warning(f"[Reddit] No subreddits found for keyword: {keyword}")
            return None
        
        # Choose a random URL from the list
        result = f"https://reddit.com{random.choice(urls)}/new"
        return result

async def find_random_subreddit_for_keyword(keyword: str = "BTC"):
    """
    Generate a subreddit URL using the search tool with `keyword`.
    It randomly chooses one of the resulting subreddit.
    """
    logging.info("[Reddit] generating subreddit target URL for keyword: %s", keyword)
    try:
        async with aiohttp.ClientSession() as session:
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            async with session.get(
                f"https://www.reddit.com/search/?q={keyword}&type=sr",
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},
                timeout=BASE_TIMEOUT
            ) as response:
                if response.status == 429 or response.status == 403:
                    logging.warning("[Reddit] Search keyword rate limited. Try using subdomain")
                    random_subdomain = random.choice(list_sub)
                    return await find_random_subreddit_for_keyword_using_sub_domain(session, f"https://{random_subdomain}/search/?q={keyword}&type=sr")
                    
                if response.status != 200:
                    logging.error(f"[Reddit] Non-200 status code for keyword: {response.status} for {keyword}")
                    return ''
                
                html_content = await response.text()
                tree = html.fromstring(html_content)
                urls = [
                    url
                    for url in tree.xpath('//a[contains(@href, "/r/")]//@href')
                    if not "/r/popular" in url
                ]
                
                if not urls:
                    logging.warning(f"[Reddit] No subreddits found for keyword: {keyword}")
                    return None
                
                # Choose a random URL from the list
                result = f"https://reddit.com{random.choice(urls)}/new"
                return result
    except Exception as e:
        logging.error(f"[Reddit] An error occurred while finding subreddit: {e}")
        return None
    finally:
        logging.info("Session closed")


async def generate_url(autonomous_subreddit_choice=0.35, keyword: str = "news"):
    random_value = random.random()
    if random_value < autonomous_subreddit_choice:
        logging.info("[Reddit] Exploration mode!")  
        return await find_random_subreddit_for_keyword(keyword)
    else:
        if random.random() < 0.35:     
            logging.info("[Reddit] Top 225 Subreddits mode!")       
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)+";"+"https://reddit.com/" + random.choice(subreddits_top_225)
        else:            
            logging.info("[Reddit] Top 1000 Subreddits mode!")
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)+";"+"https://reddit.com/" + random.choice(subreddits_top_1000)
        
        return selected_subreddit_


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

async def scrap_post(url: str, socks_port: str) -> AsyncGenerator[Item, None]:
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
            _url = url + ".json?sort=new"
            logging.info(f"[Reddit] Scraping - getting {_url}")
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            #session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
            async with session.get(_url, 
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},     
                timeout=BASE_TIMEOUT) as response:
                if response.status == 429:
                    # Choose a random subdomain
                    random_subdomain = random.choice(list_sub)
                    url_to_fetch = _url
                    if "https://reddit.com" in url_to_fetch:
                        url_to_fetch = url_to_fetch.replace("https://reddit.com", f"https://{random_subdomain}")
                    elif "https://www.reddit.com" in url_to_fetch:
                        url_to_fetch = url_to_fetch.replace("https://www.reddit.com", f"https://{random_subdomain}")
                        
                    logging.warning("[Reddit] [JSON MODE] [Try to replace with other subdomain for Sub Reddit] Rate limit encountered when scraping comment for %s.", url_to_fetch)
                    response = await fetch_subreddit_json_using_sub_domain(session, url_to_fetch, socks_port)
                    # logging.warning("[Reddit] [COMMENT SECTION] [Try to use TOR]  Scraping - getting Rate limit encountered for %s.", _url)
                    # response = await fetch_with_tor(_url, random.choice(USER_AGENT_LIST), "socks5", socks_port)
                else:
                    response = await response.json()

                #logging.info(f"Response: {response}")
                try:
                    [_post, comments] = response
                except ValueError as e:
                    logging.error(f"Error unpacking response for permalink {_url}: {str(e)}. Response content: {response}")
                    return
                except TypeError as e:
                    logging.error(f"Error unpacking response for permalink {_url}: {str(e)}. Response type: {type(response)}")
                    return
                #logging.info(f"post: {_post}")
                #logging.info(f"comments: {comments}")
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
        logging.info("Session close")
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


async def fetch_subreddit_new_layout_json(session: aiohttp.ClientSession, url: str) -> str:
    if "https:/reddit.com" in url:
        url = url.replace("https:/reddit.com", "https://reddit.com")
        
    reddit_session_cookie = await get_email(".env") 
    cookies = {'reddit_session': reddit_session_cookie}
    session.cookie_jar.update_cookies(cookies)
    async with session.get(url, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
        if response.status == 429:
            logging.warning("[Reddit] [NEW LAYOUT MODE] Rate limit encountered for %s.", url)
            # await asyncio.sleep(30)
            return await fetch_new_layout_with_proxy(session, url)
        if response.status != 200:
            logging.error(f"[Reddit] [NEW LAYOUT MODE] Non-200 status code: {response.status} for {url}")
            return ''
        return await response.text()

async def scrap_subreddit_new_layout(subreddit_urls: str) -> AsyncGenerator[str, None]:
    urls = [url.strip() for url in subreddit_urls.split(';')]
    logging.info("[Reddit] [NEW LAYOUT MODE] Opening: %s",subreddit_urls)
    reddit_session_cookie = await get_email(".env")
    cookies = {'reddit_session': reddit_session_cookie}
    async with aiohttp.ClientSession(cookies=cookies) as session:
        tasks = [fetch_subreddit_new_layout_json(session, url) for url in urls]
        html_contents = await asyncio.gather(*tasks)
        
        for html_content, url in zip(html_contents, urls):
            if html_content:
                html_tree = fromstring(html_content)
                permalinks = html_tree.xpath("//shreddit-post/@permalink")
                
                for post in permalinks:
                    post_url = post
                    if post_url.startswith("/r/"):
                        post_url = "https://www.reddit.com" + post_url
                    try:
                        lock = asyncio.Lock()
                        async for item in scrap_post(post_url, lock):
                            yield item
                    except Exception as e:
                        logging.exception(f"[Reddit] [NEW LAYOUT MODE] Error detected: {e}")


def find_permalinks(data):
    if isinstance(data, dict):
        if 'permalink' in data and is_within_timeframe_seconds(data['created_utc'], 86400) and data['num_comments'] > 2:
            yield data['permalink']
        for key, value in data.items():
            yield from find_permalinks(value)
    elif isinstance(data, list):
        for item in data:
            yield from find_permalinks(item)

async def fetch_with_proxy_using_curl(url_to_fetch, proxy):
    command = [
        'curl', '-L', '-x', proxy,
        '-H', f"User-Agent: {random.choice(USER_AGENT_LIST)}",
        url_to_fetch
    ]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if result.returncode == 0:
            logging.info(f"cURL success for {url_to_fetch} with proxy {proxy}")
            content = result.stdout.decode('utf-8')
            return json.loads(content)
        else:
            logging.error(f"cURL failed for {url_to_fetch} with proxy {proxy}")
            return {}
    except subprocess.TimeoutExpired:
        logging.error(f"cURL timeout expired for {url_to_fetch} with proxy {proxy}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"cURL returned non-JSON response for {url_to_fetch} with proxy {proxy}")
        return {}

async def fetch_with_proxy_using_pycurl(url_to_fetch, proxy):
    buffer = ()
    c = pycurl.Curl()
    try:
        c.setopt(c.URL, url_to_fetch)
        c.setopt(c.PROXY, proxy)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.USERAGENT, random.choice(USER_AGENT_LIST))
        c.setopt(c.SSL_VERIFYPEER, False)
        c.setopt(c.SSL_VERIFYHOST, False)
        c.setopt(c.TIMEOUT, 30)
        c.perform()
        
        response_code = c.getinfo(pycurl.RESPONSE_CODE)
        if response_code == 200:
            logging.info(f"pycURL success for {url_to_fetch} with proxy {proxy}")
            content = buffer.getvalue().decode('utf-8')
            return json.loads(content)
        else:
            logging.error(f"pycURL failed for {url_to_fetch} with proxy {proxy}: HTTP {response_code}")
            return {}
    except pycurl.error as e:
        logging.error(f"pycURL error for {url_to_fetch} with proxy {proxy}: {str(e)}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"pycURL returned non-JSON response for {url_to_fetch} with proxy {proxy}")
        return {}
    finally:
        c.close()

async def fetch_with_proxy(session, url_to_fetch):
    proxy = await manage_proxies()
    if proxy:
        if not "https" in proxy:
            url_to_fetch = url_to_fetch.replace("https", "http")
        logging.warning("Rate limit encountered. Retrying with proxy %s.", proxy + url_to_fetch)
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENT_LIST),
                "Accept-Encoding": "gzip, deflate",
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            async with session.get(url_to_fetch, proxy=proxy, headers=headers, timeout=30, allow_redirects=True) as proxy_response:
                if proxy_response.status == 200:
                    content_type = proxy_response.headers.get('Content-Type', '')
                    content_encoding = proxy_response.headers.get('Content-Encoding', '')
                    if 'application/json' in content_type:
                        logging.info(f"Success to fetch {url_to_fetch} with proxy: {proxy_response.status} {proxy}")
                        raw_data = await proxy_response.read()
                        if 'gzip' in content_encoding:
                            try:
                                with gzip.GzipFile(fileobj=BytesIO(raw_data)) as gzip_file:
                                    content = gzip_file.read().decode('utf-8')
                            except Exception as e:
                                logging.error(f"Failed to decompress gzip content for {url_to_fetch}: {e}")
                                content = raw_data.decode('utf-8')
                        elif 'deflate' in content_encoding:
                            try:
                                content = zlib.decompress(raw_data).decode('utf-8')
                            except Exception as e:
                                logging.error(f"Failed to decompress deflate content for {url_to_fetch}: {e}")
                                content = None
                        else:
                            content = raw_data.decode('utf-8')
        
                        if content:
                            json_data = json.loads(content)
                            return json_data
                        else:
                            remove_proxies(proxy)
                            return {}
                    else:
                        remove_proxies(proxy)
                        logging.error(f"Unexpected content type: {content_type}, URL: {url_to_fetch}")
                        return {}
                else:
                    # try_curl = await fetch_with_proxy_using_curl(url_to_fetch, proxy)
                    try_curl = await fetch_with_proxy_using_curl(url_to_fetch, proxy)
                    if try_curl:
                        return try_curl
                    else:
                        remove_proxies(proxy)
                        logging.error(f"Failed to fetch {url_to_fetch} with proxy: {proxy_response.status}")
                        return {}
                        
        except asyncio.TimeoutError:
            remove_proxies(proxy)
            logging.error(f"Timeout occurred on attempt for URL {url_to_fetch} with proxy {proxy}")
            return {}
        except aiohttp.ClientOSError as e:
            remove_proxies(proxy)
            logging.error(f"ClientOSError on attempt for URL {url_to_fetch} with proxy {proxy}")
            return {}
        except ServerDisconnectedError as e:
            remove_proxies(proxy)
            logging.error(f"ServerDisconnectedError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return {}
        except ClientHttpProxyError as e:
            remove_proxies(proxy)
            logging.error(f"ClientHttpProxyError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return {}
        except ClientError as e:
            remove_proxies(proxy)
            logging.error(f"ClientError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return {}
        except Exception as e:
            logging.error(f"Unexpected error on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
                
    else:
        logging.error(f"Proxies not found")
        return {}

async def fetch_new_layout_with_proxy(session, url_to_fetch):
    proxy = await manage_proxies()
    if proxy:
        if not "https" in proxy:
            url_to_fetch = url_to_fetch.replace("https", "http")
        logging.warning("Rate limit encountered. Retrying with proxy %s.", proxy + url_to_fetch)
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENT_LIST),
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            async with session.get(url_to_fetch, proxy=proxy, headers=headers, timeout=30, allow_redirects=True) as proxy_response:
                if proxy_response.status == 200:
                    content_type = proxy_response.headers.get('Content-Type', '')
                    if 'application/json' in content_type:
                        logging.info(f"Success to fetch {url_to_fetch} with proxy: {proxy_response.status} {proxy}")
                        json_data = await proxy_response.text()       
                        return json_data
                    else:
                        remove_proxies(proxy)
                        logging.error(f"Unexpected content type: {content_type}, URL: {url_to_fetch}")
                        return ''
                else:
                    remove_proxies(proxy)
                    logging.error(f"Failed to fetch {url_to_fetch} with proxy: {proxy_response.status}")
                    return ''
        except asyncio.TimeoutError:
            remove_proxies(proxy)
            logging.error(f"Timeout occurred on attempt for URL {url_to_fetch} with proxy {proxy}")
            return ''
        except aiohttp.ClientOSError as e:
            remove_proxies(proxy)
            logging.error(f"ClientOSError on attempt for URL {url_to_fetch} with proxy {proxy}")
            return ''
        except ServerDisconnectedError as e:
            remove_proxies(proxy)
            logging.error(f"ServerDisconnectedError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return ''
        except ClientHttpProxyError as e:
            remove_proxies(proxy)
            logging.error(f"ClientHttpProxyError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return ''
        except ClientError as e:
            remove_proxies(proxy)
            logging.error(f"ClientError on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
            return ''
        except Exception as e:
            logging.error(f"Unexpected error on attempt for URL {url_to_fetch} with proxy {proxy}: {e}")
                
    else:
        logging.error(f"Proxies not found")
        return ''
    
async def fetch_subreddit_json_using_sub_domain_and_curl(subreddit_url: str):
    url_to_fetch = subreddit_url
    command = [
        'curl', '-L', '-s', 
        "--max-time", str(BASE_TIMEOUT), 
        '-H', f"User-Agent: {random.choice(USER_AGENT_LIST)}",
        url_to_fetch
    ]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if result.returncode == 0:
            logging.info(f"cURL success for {url_to_fetch} ")
            content = result.stdout.decode('utf-8')
            return json.loads(content)
        else:
            logging.error(f"cURL failed for {url_to_fetch} ")
            return {}
    except subprocess.TimeoutExpired:
        logging.error(f"cURL timeout expired for {url_to_fetch} ")
        return {}
    except json.JSONDecodeError:
        logging.error(f"cURL returned non-JSON response for {url_to_fetch}")
        return {}
    
async def fetch_subreddit_json_using_sub_domain_curl(subreddit_url: str, socks_port: str) -> dict:
    url_to_fetch = subreddit_url
    logging.info("[Reddit] [JSON MODE with Sub Domain] opening: %s", url_to_fetch)
    
    user_agent = random.choice(USER_AGENT_LIST)
    curl_command = [
        "curl",
        "-s",  # Silent mode
        "-i",  # Include headers to check status code
        "-L",  # Follow redirects
        "--max-time", str(BASE_TIMEOUT),  # Set timeout
        "-H", f"User-Agent: {user_agent}",
        url_to_fetch
    ]
    
    try:
        result = subprocess.run(curl_command, capture_output=True, text=True, check=True)
        
        # Check headers for status code
        headers, body = result.stdout.split('\r\n\r\n', 1)
        status_match = re.search(r'HTTP/\d\.\d (\d+)', headers)
        status_code = int(status_match.group(1)) if status_match else None
        
        if status_code is None:
            logging.error(f"Could not determine HTTP status code from headers for {url_to_fetch}")
            return {}
        
        if status_code == 429:
            logging.warning("[Reddit] [JSON MODE] [Try to use TOR for Sub Reddit] Rate limit encountered even using subdomain for %s.", url_to_fetch)
            return await fetch_with_tor(url_to_fetch, socks_port)
        
        if status_code != 200:
            logging.error(f"[Reddit] [JSON MODE] Non-200 status code for subdomain: {status_code} for {url_to_fetch}")
            return {}
        
        # Check Content-Type
        content_type = next((line.split(':')[1].strip() for line in headers.split('\r\n') if line.lower().startswith('content-type:')), None)
        if content_type and 'json' not in content_type.lower():
            logging.error(f"Expected JSON, got {content_type} for {url_to_fetch}")
            return {}
        
        # Parse JSON
        try:
            if not body.strip():
                logging.error(f"Empty response body from {url_to_fetch}")
                return {}
            json_data = json.loads(body)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding error: {e} for {url_to_fetch}")
            logging.debug(f"Content attempted to parse: {body}")
            return {}
        except Exception as e:
            logging.error(f"Unexpected error parsing response as JSON: {e} for {url_to_fetch}")
            return {}
        
        return json_data
    
    except subprocess.CalledProcessError as e:
        logging.error(f"curl command failed with exit code {e.returncode} for {url_to_fetch}")
        if e.returncode == 429:
            return await fetch_with_tor(url_to_fetch, random.choice(USER_AGENT_LIST), "socks5", socks_port)
        return {}
    except subprocess.SubprocessError as e:
        logging.error(f"Subprocess error: {e} for {url_to_fetch}")
        return {}
    except ValueError as e:
        logging.error(f"Error parsing response status: {e} for {url_to_fetch}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e} for {url_to_fetch}")
        return {}
    
async def fetch_subreddit_json_using_sub_domain(session: aiohttp.ClientSession, subreddit_url: str, socks_port: str) -> dict:
    url_to_fetch = subreddit_url
    logging.info("[Reddit] [JSON MODE with Sub Domain] opening: %s", url_to_fetch)
    reddit_session_cookie = await get_email(".env") 
    cookies = {'reddit_session': reddit_session_cookie}
    session.cookie_jar.update_cookies(cookies)
    async with session.get(url_to_fetch, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
        if response.status == 429:
            logging.warning("[Reddit] [JSON MODE] [Try to use TOR for Sub Reddit] Rate limit encountered even using subdomain for %s.", url_to_fetch)
            return await fetch_with_tor(url_to_fetch, random.choice(USER_AGENT_LIST), "socks5", socks_port)
            
        if response.status != 200:
            logging.error(f"[Reddit] [JSON MODE] Non-200 status code for subdomain: {response.status} for {url_to_fetch}")
            return {}
        return await response.json()
        
async def fetch_subreddit_json(session: aiohttp.ClientSession, subreddit_url: str, socks_port: str) -> dict:
    url_to_fetch = subreddit_url
    if "https:/reddit.com" in url_to_fetch:
        url_to_fetch = url_to_fetch.replace("https:/reddit.com", "https://reddit.com")
        
    if random.random() < 0.75:
        url_to_fetch = url_to_fetch + "/new"
    url_to_fetch = url_to_fetch + "/.json"
        
    if url_to_fetch.endswith("/new/new/.json"):
        url_to_fetch = url_to_fetch.replace("/new/new/.json", "/new/.json")
    
    logging.info("[Reddit] [JSON MODE] opening: %s", url_to_fetch)
    reddit_session_cookie = await get_email(".env") 
    cookies = {'reddit_session': reddit_session_cookie}
    session.cookie_jar.update_cookies(cookies)
    async with session.get(url_to_fetch, headers={"User-Agent": random.choice(USER_AGENT_LIST)}, timeout=BASE_TIMEOUT) as response:
        if response.status == 429:
            # Choose a random subdomain
            random_subdomain = random.choice(list_sub)
            url_to_fetch = subreddit_url
            
            # Replace the domain with the chosen subdomain
            if "https://reddit.com" in url_to_fetch:
                url_to_fetch = url_to_fetch.replace("https://reddit.com", f"https://{random_subdomain}")
            elif "https://www.reddit.com" in url_to_fetch:
                url_to_fetch = url_to_fetch.replace("https://www.reddit.com", f"https://{random_subdomain}")
                
            if random.random() < 0.75:
                url_to_fetch = url_to_fetch + "/new"
            url_to_fetch = url_to_fetch + "/.json"
                
            if url_to_fetch.endswith("/new/new/.json"):
                url_to_fetch = url_to_fetch.replace("/new/new/.json", "/new/.json")
                
            logging.warning("[Reddit] [JSON MODE] [Try to replace with other subdomain for Sub Reddit] Rate limit encountered for %s.", url_to_fetch)
            return await fetch_subreddit_json_using_sub_domain(session, url_to_fetch, socks_port)
            
        if response.status != 200:
            logging.error(f"[Reddit] [JSON MODE] Non-200 status code: {response.status} for {url_to_fetch}")
            return {}
        return await response.json()

async def fetch_and_scrap_post(permalink, socks_port: str):
    post_url = permalink
    if not post_url.startswith("https://"):
        post_url = f"https://reddit.com{post_url}"
    items = []
    try:
        async for item in scrap_post(post_url, socks_port):
            items.append(item)
    except Exception as e:
        logging.exception(f"[Reddit] [JSON MODE] Error detected: {e} {post_url}")
    return items

async def scrap_subreddit_json(subreddit_urls: str) -> AsyncGenerator[str, None]:
    urls = [url.strip() for url in subreddit_urls.split(';')]
    reddit_session_cookie = await get_email(".env")
    cookies = {'reddit_session': reddit_session_cookie}
    logging.info("[Reddit] [JSON MODE] opening urls: %s", urls)
    async with aiohttp.ClientSession(cookies=cookies) as session:
        lock = asyncio.Lock()
        tasks = [fetch_subreddit_json(session, url, random.choice(TOR_PORTS)) for url in urls]
        json_responses = await asyncio.gather(*tasks)
        
        for data, url in zip(json_responses, urls):
            if data:
                permalinks = list(find_permalinks(data))
                tasks = []
                lock = asyncio.Lock()
                for permalink in permalinks:
                    logging.warning("[Reddit] [JSON MODE] find permalink, add to tasks %s.", permalink)
                    tasks.append(fetch_and_scrap_post(permalink, random.choice(TOR_PORTS)))
            
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(f"[Reddit] [JSON MODE] Error in task: {result}")
                    else:
                        for item in result:
                            yield item
                # for permalink in permalinks:
                #     logging.warning("[Reddit] [JSON MODE] find permalink, check post probability for %s.", permalink)
                #     #if random.random() < SKIP_POST_PROBABILITY:
                #     post_url = permalink
                #     if not post_url.startswith("https://"):
                #         post_url = f"https://reddit.com{post_url}"
                #     try:
                #         async for item in scrap_post(post_url):
                #             yield item
                #     except Exception as e:
                #         logging.exception(f"[Reddit] [JSON MODE] Error detected: {e}")



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
        if len(item['content'])>10:
            subreddit_name = extract_subreddit_name(item["url"])
            if subreddit_name is None:
                return item
            segmented_subreddit_strs = segment(subreddit_name)
            segmented_subreddit_name = " ".join(segmented_subreddit_strs)
            item["content"] = item["content"] # + ". - " + segmented_subreddit_name + " ," + subreddit_name
    except Exception as e:
        logging.exception(f"[Reddit post_process_item] Word segmentation failed: {e}, ignoring...")
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
    await asyncio.sleep(random.uniform(0, 1))
    for i in range(nb_subreddit_attempts):
        await asyncio.sleep(random.uniform(1, i))
        url = await generate_url(**parameters["url_parameters"])
        await manage_proxies()
        # if url ends with "/new/new/.json", replace it with "/new.json"
        if url.endswith("/new/new/.json"):
            url = url.replace("/new/new/.json", "/new.json")
        logging.info(f"[Reddit] Attempt {(i+1)}/{nb_subreddit_attempts} Scraping {url} with max oldness of {max_oldness_seconds}")
        if "reddit.com" not in url:
            raise ValueError(f"Not a Reddit URL {url}")
        if url == '':
            raise ValueError("URL is empty")
            
        url_parameters = url.split("reddit.com")[1].split("/")[1:]
        if "comments" in url_parameters:
            socks_port = random.choice(TOR_PORTS)
            async for result in scrap_post(url, socks_port):

                yielded_items += 1
                result = post_process_item(result)
                if is_valid_item(result, min_post_length):
                    logging.info(f"[Reddit] Found Reddit post: {result}")
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
                    logging.info(f"[Reddit] Found Reddit comment: {result}")
                    yield result
                if yielded_items >= MAXIMUM_ITEMS_TO_COLLECT:
                    break
