import random
import aiohttp
import dotenv
import os
import asyncio
from lxml import html
from typing import AsyncGenerator
import time
from datetime import time as tttime, datetime as datett
from datetime import timezone
import pytz
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
    if start_time <= now_utc < end_time:
        default_var = await load_env_variable("SCWEET_USERNAME", none_allowed=True)
        if len(default_var) < 70:
            default_var = await load_env_variable("SCWEET_EMAIL", none_allowed=True)
    else:
        default_var = await load_env_variable("SCWEET_EMAIL", none_allowed=True)
    
    return default_var


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


async def find_random_subreddit_for_keyword(keyword: str = "BTC"):
    """
    Generate a subreddit URL using the search tool with `keyword`.
    It randomly chooses one of the resulting subreddit.
    """
    logging.info("[Reddit] generating subreddit target URL.")
    try:
        async with aiohttp.ClientSession() as session:
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            #session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
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
        logging.info("Session close")
        await session.close()


async def generate_url(autonomous_subreddit_choice=0.35, keyword: str = "BTC"):
    random_value = random.random()
    if random_value < autonomous_subreddit_choice:
        logging.info("[Reddit] Exploration mode!")  
        return await find_random_subreddit_for_keyword(keyword)
    else:
        if random.random() < 0.5:     
            logging.info("[Reddit] Top 225 Subreddits mode!")       
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_225)
        else:            
            logging.info("[Reddit] Top 1000 Subreddits mode!")
            selected_subreddit_ = "https://reddit.com/" + random.choice(subreddits_top_1000)
        
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
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            #session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
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


async def scrap_subreddit_new_layout(subreddit_url: str) -> AsyncGenerator[Item, None]:
    
    try:
        async with aiohttp.ClientSession() as session:
            url_to_fetch = subreddit_url
            logging.info("[Reddit] [NEW LAYOUT MODE] Opening: %s",url_to_fetch)
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            #session.cookie_jar.update_cookies({'reddit_session': reddit_session_cookie, 'domain': '.reddit.com'})
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
        logging.info("Session close")
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
            url_to_fetch = subreddit_url
            if random.random() < 0.75:
                url_to_fetch = url_to_fetch + "/new"
            url_to_fetch = url_to_fetch + "/.json"
                
            if url_to_fetch.endswith("/new/new/.json"):
                url_to_fetch = url_to_fetch.replace("/new/new/.json", "/new.json")
            logging.info("[Reddit] [JSON MODE] opening: %s",url_to_fetch)
            reddit_session_cookie = await get_email(".env") 
            cookies = {'reddit_session': reddit_session_cookie}
            session.cookie_jar.update_cookies(cookies)
            await asyncio.sleep(2)
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
        logging.info("Session close")
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
        if len(item['content'])>10:
            subreddit_name = extract_subreddit_name(item["url"])
            if subreddit_name is None:
                return item
            segmented_subreddit_strs = segment(subreddit_name)
            segmented_subreddit_name = " ".join(segmented_subreddit_strs)
            item["content"] = item["content"] + ". - " + segmented_subreddit_name + " ," + subreddit_name
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

    
    await asyncio.sleep(random.uniform(3, 15))
    for i in range(nb_subreddit_attempts):
        await asyncio.sleep(random.uniform(1, i))
        url = await generate_url(**parameters["url_parameters"])
        # if url ends with "/new/new/.json", replace it with "/new.json"
        if url.endswith("/new/new/.json"):
            url = url.replace("/new/new/.json", "/new.json")
        logging.info(f"[Reddit] Attempt {(i+1)}/{nb_subreddit_attempts} Scraping {url} with max oldness of {max_oldness_seconds}")
        if "reddit.com" not in url:
            raise ValueError(f"Not a Reddit URL {url}")
        url_parameters = url.split("reddit.com")[1].split("/")[1:]
        if "comments" in url_parameters:
            async for result in scrap_post(url):

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
