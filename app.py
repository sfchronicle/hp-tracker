import os
import re
import time
from datetime import datetime

import gspread
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from gspread_dataframe import set_with_dataframe

# We create a dictionary of the markets we want to track.
markets = {
    "San Antonio": {
        "url": "https://www.expressnews.com",
        "timezone": "US/Central",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/1F073i7iMDEU0q2B8K3nG881YDr-f1bCXzc2h24V_dOs/edit?usp=sharing",
    },
    "Houston": {
        "url": "https://www.houstonchronicle.com",
        "timezone": "US/Central",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/19IZkVDucvXYT2EyHQ-8Yu2MSrknYK3Co_HI3TAd_hBA/edit#gid=0",
    },
    "San Francisco": {
        "url": "https://www.sfchronicle.com",
        "timezone": "US/Pacific",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/1YhvmHOeT5RQLmoef6zZhWfwLD87JIAFiGqzS_llbTu8/edit#gid=0",
    },
    "Albany": {
        "url": "https://www.timesunion.com",
        "timezone": "US/Eastern",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/1NREkjXsMslgsl_8XaS-9W_3t3gU67jfmptoN9-9yt1k/edit#gid=1676782739",
    },
    "Connecticut Insider": {
        "url": "https://www.ctinsider.com",
        "timezone": "US/Eastern",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/10V626AzMp1NXaW4wOnUq_VArl79XpCdbJQkbIk9esGA/edit#gid=964675505",
    },
    "Connecticut Post": {
        "url": "https://www.ctpost.com",
        "timezone": "US/Eastern",
        "spreadsheet": "https://docs.google.com/spreadsheets/d/1wMvD70EZO27TyzFY80cOxZPdFTCSqnB4YMarwuf-1vI/edit#gid=0",
    },
}

# We grab our service account from a Github secret
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open("service_account.json", "w") as f:
    f.write(SERVICE_ACCOUNT)

# We authenticate with Google using the service account json we created earlier.
gc = gspread.service_account(filename="service_account.json")


def remove_duplicate_prefix(url, prefix):
    if url.startswith(prefix * 2):
        url = url.replace(prefix, "", 1)
    return url


# This handy dandy function will retry the api call if it fails.
def api_call_handler(func):
    # Number of retries
    for i in range(0, 6):
        try:
            return func()
        except Exception as e:
            print(f"🤦‍♂️ {e}")
            print(f"🤷‍♂️ Retrying in {2 ** i} seconds...")
            time.sleep(2**i)
    print("🤬 Giving up...")
    raise SystemError


def getSoup(url):
    """
    This function takes a URL and returns a BeautifulSoup object.
    """
    headers = {
        "x-px-access-token": ACCESS_TOKEN,
    }
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    return soup


def get_san_antonio_headlines():
    """
    This function scrapes the San Antonio Express-News homepage
    """

    market_url = "https://www.expressnews.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="

    # Get the HTML content of the homepage
    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all("div", class_="centerpiece-tab--main-headline")

    # We extract the text from the headlines and strip the whitespace.
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_url = remove_duplicate_prefix(
        market_url + cp_headlines[0].find("a")["href"], market_url
    )
    tab2_url = remove_duplicate_prefix(
        market_url + cp_headlines[1].find("a")["href"], market_url
    )
    tab3_url = remove_duplicate_prefix(
        market_url + cp_headlines[2].find("a")["href"], market_url
    )
    tab4_url = remove_duplicate_prefix(
        market_url + cp_headlines[3].find("a")["href"], market_url
    )
    tab5_url = remove_duplicate_prefix(
        market_url + cp_headlines[4].find("a")["href"], market_url
    )
    tab6_url = remove_duplicate_prefix(
        market_url + cp_headlines[5].find("a")["href"], market_url
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_centerpiece_tab"
    # Example: class="hide-rss-link hdnce-e hdnce-collection-105803-dynamic_centerpiece_tab"
    cp_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_centerpiece_tab")
    )

    cp_tab_id_list = []
    for tab in cp_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        cp_tab_id_list.append(f"{wcm_url}{tab_id}")

    (
        cp_order,
        tab2_order,
        tab3_order,
        tab4_order,
        tab5_order,
        tab6_order,
    ) = cp_tab_id_list

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar.
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = remove_duplicate_prefix(
            market_url + breaking_headlines[0]["href"], market_url
        )
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = remove_duplicate_prefix(
            market_url + breaking_headlines[1]["href"], market_url
        )
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
    except:
        just_in_headline = None
        just_in_url = None

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines list and extract the text from the headlines, stripping the whitespace.
    top_headlines_list = soup.find("ul", class_="coreHeadlineList--items")

    headline_list = top_headlines_list.find_all(
        "div", class_="coreHeadlineList--item-headline"
    )
    top1 = headline_list[0].text.strip()
    top2 = headline_list[1].text.strip()
    top3 = headline_list[2].text.strip()
    top4 = headline_list[3].text.strip()
    top5 = headline_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url + headline_list[0].find("a")["href"], market_url
    )
    top2_url = remove_duplicate_prefix(
        market_url + headline_list[1].find("a")["href"], market_url
    )
    top3_url = remove_duplicate_prefix(
        market_url + headline_list[2].find("a")["href"], market_url
    )
    top4_url = remove_duplicate_prefix(
        market_url + headline_list[3].find("a")["href"], market_url
    )
    top5_url = remove_duplicate_prefix(
        market_url + headline_list[4].find("a")["href"], market_url
    )

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_headline_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Print out all the headlines in a formatted string.
    # print(
    #     f"""
    # 📰 Breaking 1: {breaking1_headline}
    # 📰 Breakinhg 2: {breaking2_headline}
    # 📰 Just in: {just_in_headline}
    # 📰 CP: {cp_headline}
    # 📰 Tab 2: {tab2_headline}
    # 📰 Tab 3: {tab3_headline}
    # 📰 Tab 4: {tab4_headline}
    # 📰 Tab 5: {tab5_headline}
    # 📰 Tab 6: {tab6_headline}
    # 📰 Headline 1: {top1}
    # 📰 Headline 2: {top2}
    # 📰 Headline 3: {top3}
    # 📰 Headline 4: {top4}
    # 📰 Headline 5: {top5}"""
    # )

    # # Print the tab order
    # print(
    #     f"""
    #     🎌 Breaking 1 Order: {breaking1_order}
    #     🎌 Breaking 2 Order: {breaking2_order}
    #     🎌 Just In Order: {just_in_order}
    #     🎌 CP Order: {cp_order}
    #     🎌 Tab 2 Order: {tab2_order}
    #     🎌 Tab 3 Order: {tab3_order}
    #     🎌 Tab 4 Order: {tab4_order}
    #     🎌 Tab 5 Order: {tab5_order}
    #     🎌 Tab 6 Order: {tab6_order}
    #     🎌 Top Headlines Order: {top_headline_order}
    #       """
    # )

    # # Print out all the headlines
    # print(
    #     f"""
    #     🔗 Breaking 1 URL: {breaking1_url}
    #     🔗 Breaking 2 URL: {breaking2_url}
    #     🔗 Just In URL: {just_in_url}
    #     🔗 CP URL: {cp_url}
    #     🔗 Tab 2 URL: {tab2_url}
    #     🔗 Tab 3 URL: {tab3_url}
    #     🔗 Tab 4 URL: {tab4_url}
    #     🔗 Tab 5 URL: {tab5_url}
    #     🔗 Tab 6 URL: {tab6_url}
    #     🔗 Top 1 URL: {top1_url}
    #     🔗 Top 2 URL: {top2_url}
    #     🔗 Top 3 URL: {top3_url}
    #     🔗 Top 4 URL: {top4_url}
    #     🔗 Top 5 URL: {top5_url}
    #     """
    # )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df


def get_houston_headlines():
    """
    This function scrapes the Houston Chronicle homepage
    """
    # Get the HTML content of the Houston Chronicle homepage
    market_url = "https://www.houstonchronicle.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="

    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all("div", class_="centerpiece-tab--main-headline")

    # We extract the text from the headlines and strip the whitespace.
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_url = remove_duplicate_prefix(
        market_url + cp_headlines[0].find("a")["href"], market_url
    )
    tab2_url = remove_duplicate_prefix(
        market_url + cp_headlines[1].find("a")["href"], market_url
    )
    tab3_url = remove_duplicate_prefix(
        market_url + cp_headlines[2].find("a")["href"], market_url
    )
    tab4_url = remove_duplicate_prefix(
        market_url + cp_headlines[3].find("a")["href"], market_url
    )
    tab5_url = remove_duplicate_prefix(
        market_url + cp_headlines[4].find("a")["href"], market_url
    )
    tab6_url = remove_duplicate_prefix(
        market_url + cp_headlines[5].find("a")["href"], market_url
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_centerpiece_tab"
    # Example: class="hide-rss-link hdnce-e hdnce-collection-105803-dynamic_centerpiece_tab"
    cp_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_centerpiece_tab")
    )

    cp_tab_id_list = []
    for tab in cp_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        cp_tab_id_list.append(f"{wcm_url}{tab_id}")

    (
        cp_order,
        tab2_order,
        tab3_order,
        tab4_order,
        tab5_order,
        tab6_order,
    ) = cp_tab_id_list

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar.
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = remove_duplicate_prefix(
            market_url + breaking_headlines[0]["href"], market_url
        )
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = remove_duplicate_prefix(
            market_url + breaking_headlines[1]["href"], market_url
        )
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
    except:
        just_in_headline = None
        just_in_url = None

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines list and extract the text from the headlines, stripping the whitespace.
    top_headlines_list = soup.find("ul", class_="coreHeadlineList--items")

    headline_list = top_headlines_list.find_all(
        "div", class_="coreHeadlineList--item-headline"
    )
    top1 = headline_list[0].text.strip()
    top2 = headline_list[1].text.strip()
    top3 = headline_list[2].text.strip()
    top4 = headline_list[3].text.strip()
    top5 = headline_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url + headline_list[0].find("a")["href"], market_url
    )
    top2_url = remove_duplicate_prefix(
        market_url + headline_list[1].find("a")["href"], market_url
    )
    top3_url = remove_duplicate_prefix(
        market_url + headline_list[2].find("a")["href"], market_url
    )
    top4_url = remove_duplicate_prefix(
        market_url + headline_list[3].find("a")["href"], market_url
    )
    top5_url = remove_duplicate_prefix(
        market_url + headline_list[4].find("a")["href"], market_url
    )

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_headline_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["Houston"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["Houston"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df


def get_albany_headlines():
    """
    This function scrapes the Albany Times Union homepage
    """
    market_url = "https://www.timesunion.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="
    # Get the HTML content of the page
    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find the headlines in the "Capital Region News" section
    cp_headlines = soup.find_all("a", class_="dynamicSpotlight--item-header")

    # Extract the text from the headlines and strip the whitespace
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()
    tab7_headline = cp_headlines[6].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_headlines = soup.find_all("a", class_="dynamicSpotlight--item-header")
    cp_url = remove_duplicate_prefix(market_url + cp_headlines[0]["href"], market_url)
    tab2_url = remove_duplicate_prefix(market_url + cp_headlines[1]["href"], market_url)
    tab3_url = remove_duplicate_prefix(market_url + cp_headlines[2]["href"], market_url)
    tab4_url = remove_duplicate_prefix(market_url + cp_headlines[3]["href"], market_url)
    tab5_url = remove_duplicate_prefix(market_url + cp_headlines[4]["href"], market_url)
    tab6_url = remove_duplicate_prefix(market_url + cp_headlines[5]["href"], market_url)
    tab7_url = remove_duplicate_prefix(market_url + cp_headlines[6]["href"], market_url)

    # Replace "?IPID=Times-Union-HP-spotlight" with an empty string.
    cp_url = cp_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab2_url = tab2_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab3_url = tab3_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab4_url = tab4_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab5_url = tab5_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab6_url = tab6_url.replace("?IPID=Times-Union-HP-spotlight", "")
    tab7_url = tab7_url.replace("?IPID=Times-Union-HP-spotlight", "")

    cp_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab2_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab3_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab4_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab5_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab6_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )
    tab7_order = (
        "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614"
    )

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = f'{market_url}{breaking_headlines[0]["href"]}'
        breaking1_url = breaking1_url.replace("?IPID=Times-Union-HP-breaking-bar", "")
        breaking1_url = remove_duplicate_prefix(breaking1_url, market_url)
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = f'{market_url}{breaking_headlines[1]["href"]}'
        breaking2_url = breaking2_url.replace("?IPID=Times-Union-HP-breaking-bar", "")
        breaking2_url = remove_duplicate_prefix(breaking2_url, market_url)
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
        # Remove "?IPID=Times-Union-HP-just-in" from the URL
        just_in_url = just_in_url.replace("?IPID=Times-Union-HP-just-in", "")

        just_in_url = remove_duplicate_prefix(just_in_url, market_url)
    except:
        just_in_headline = None
        just_in_url = None

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines in the "Capital Region News" section
    top_headlines_list = soup.find("div", class_="thumbnail-list-wrapper")
    top_headlines_list = top_headlines_list.find_all("li")

    # Extract the text from the top headlines and strip the whitespace
    top1 = top_headlines_list[0].text.strip()
    top2 = top_headlines_list[1].text.strip()
    top3 = top_headlines_list[2].text.strip()
    top4 = top_headlines_list[3].text.strip()
    top5 = top_headlines_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url + top_headlines_list[0].find("a")["href"], market_url
    )
    top2_url = remove_duplicate_prefix(
        market_url + top_headlines_list[1].find("a")["href"], market_url
    )
    top3_url = remove_duplicate_prefix(
        market_url + top_headlines_list[2].find("a")["href"], market_url
    )
    top4_url = remove_duplicate_prefix(
        market_url + top_headlines_list[3].find("a")["href"], market_url
    )
    top5_url = remove_duplicate_prefix(
        market_url + top_headlines_list[4].find("a")["href"], market_url
    )

    # Replace "?IPID=Times-Union-HP-latest-news" with an empty string.
    top1_url = top1_url.replace("?IPID=Times-Union-HP-latest-news", "")
    top2_url = top2_url.replace("?IPID=Times-Union-HP-latest-news", "")
    top3_url = top3_url.replace("?IPID=Times-Union-HP-latest-news", "")
    top4_url = top4_url.replace("?IPID=Times-Union-HP-latest-news", "")
    top5_url = top5_url.replace("?IPID=Times-Union-HP-latest-news", "")

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_thumbnail_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["Albany"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["Albany"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Tab 7": tab7_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Tab 7": tab7_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Tab 7": tab7_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    # # Print the headlines
    # print(
    #     f"""
    # 📰 Breaking 1: {breaking1_headline}
    # 📰 Breaking 2: {breaking2_headline}
    # 📰 Just in: {just_in_headline}
    # 📰 CP: {cp_headline}
    # 📰 Tab 2: {tab2_headline}
    # 📰 Tab 3: {tab3_headline}
    # 📰 Tab 4: {tab4_headline}
    # 📰 Tab 5: {tab5_headline}
    # 📰 Tab 6: {tab6_headline}
    # 📰 Tab 7: {tab7_headline}
    # 📰 Headline 1: {top1}
    # 📰 Headline 2: {top2}
    # 📰 Headline 3: {top3}
    # 📰 Headline 4: {top4}
    # 📰 Headline 5: {top5}"""
    # )

    # # Print out all the headlines
    # print(
    #     f"""
    #     🔗 Breaking 1 URL: {breaking1_url}
    #     🔗 Breaking 2 URL: {breaking2_url}
    #     🔗 Just In URL: {just_in_url}
    #     🔗 CP URL: {cp_url}
    #     🔗 Tab 2 URL: {tab2_url}
    #     🔗 Tab 3 URL: {tab3_url}
    #     🔗 Tab 4 URL: {tab4_url}
    #     🔗 Tab 5 URL: {tab5_url}
    #     🔗 Tab 6 URL: {tab6_url}
    #     🔗 Tab 7 URL: {tab7_url}
    #     🔗 Top 1 URL: {top1_url}
    #     🔗 Top 2 URL: {top2_url}
    #     🔗 Top 3 URL: {top3_url}
    #     🔗 Top 4 URL: {top4_url}
    #     🔗 Top 5 URL: {top5_url}
    #     """
    # )

    # # Print the tab order
    # print(
    #     f"""
    #     🎌 Breaking 1 Order: {breaking1_order}
    #     🎌 Breaking 2 Order: {breaking2_order}
    #     🎌 Just In Order: {just_in_order}
    #     🎌 CP Order: {cp_order}
    #     🎌 Tab 2 Order: {tab2_order}
    #     🎌 Tab 3 Order: {tab3_order}
    #     🎌 Tab 4 Order: {tab4_order}
    #     🎌 Tab 5 Order: {tab5_order}
    #     🎌 Tab 6 Order: {tab6_order}
    #     🎌 Tab 7 Order: {tab7_order}
    #     🎌 Top Headlines Order: {top_headline_order}
    #       """
    # )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df


def get_san_francisco_headlines():
    """
    This function scrapes the San Francisco Chronicle homepage
    """
    market_url = "https://www.sfchronicle.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="
    # Get the HTML content of the page
    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all("div", class_="centerpiece-tab--main-headline")

    # Certain markets, like the Albany Times Union, use a different template for their homepage. In this scenario, we need to use a different class to find the headlines.
    if cp_headlines == []:
        cp_headlines = soup.find_all("a", class_="dynamicSpotlight--item-header")

    # We extract the text from the headlines and strip the whitespace.
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_url = remove_duplicate_prefix(
        market_url + cp_headlines[0].find("a")["href"], market_url
    )
    tab2_url = remove_duplicate_prefix(
        market_url + cp_headlines[1].find("a")["href"], market_url
    )
    tab3_url = remove_duplicate_prefix(
        market_url + cp_headlines[2].find("a")["href"], market_url
    )
    tab4_url = remove_duplicate_prefix(
        market_url + cp_headlines[3].find("a")["href"], market_url
    )
    tab5_url = remove_duplicate_prefix(
        market_url + cp_headlines[4].find("a")["href"], market_url
    )
    tab6_url = remove_duplicate_prefix(
        market_url + cp_headlines[5].find("a")["href"], market_url
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_centerpiece_tab"
    # Example: class="hide-rss-link hdnce-e hdnce-collection-105803-dynamic_centerpiece_tab"
    cp_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_centerpiece_tab")
    )

    cp_tab_id_list = []
    for tab in cp_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        cp_tab_id_list.append(f"{wcm_url}{tab_id}")

    (
        cp_order,
        tab2_order,
        tab3_order,
        tab4_order,
        tab5_order,
        tab6_order,
    ) = cp_tab_id_list

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar.
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = remove_duplicate_prefix(
            market_url + breaking_headlines[0]["href"], market_url
        )
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = remove_duplicate_prefix(
            market_url + breaking_headlines[1]["href"], market_url
        )
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
    except:
        just_in_headline = None
        just_in_url = None

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines list and extract the text from the headlines
    top_headlines_list = soup.find("ul", class_="coreHeadlineList--items")
    headline_list = top_headlines_list.find_all(
        "div", class_="coreHeadlineList--item-headline"
    )
    top1 = headline_list[0].text.strip()
    top2 = headline_list[1].text.strip()
    top3 = headline_list[2].text.strip()
    top4 = headline_list[3].text.strip()
    top5 = headline_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url + headline_list[0].find("a")["href"], market_url
    )
    top2_url = remove_duplicate_prefix(
        market_url + headline_list[1].find("a")["href"], market_url
    )
    top3_url = remove_duplicate_prefix(
        market_url + headline_list[2].find("a")["href"], market_url
    )
    top4_url = remove_duplicate_prefix(
        market_url + headline_list[3].find("a")["href"], market_url
    )
    top5_url = remove_duplicate_prefix(
        market_url + headline_list[4].find("a")["href"], market_url
    )

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_headline_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df

    # # Print the headlines
    # print(
    #     f"""
    # 📰 Breaking 1: {breaking1_headline}
    # 📰 Breakinhg 2: {breaking2_headline}
    # 📰 Just in: {just_in_headline}
    # 📰 CP: {cp_headline}
    # 📰 Tab 2: {tab2_headline}
    # 📰 Tab 3: {tab3_headline}
    # 📰 Tab 4: {tab4_headline}
    # 📰 Tab 5: {tab5_headline}
    # 📰 Tab 6: {tab6_headline}
    # 📰 Headline 1: {top1}
    # 📰 Headline 2: {top2}
    # 📰 Headline 3: {top3}
    # 📰 Headline 4: {top4}
    # 📰 Headline 5: {top5}"""
    # )

    # # Print the tab order
    # print(
    #     f"""
    #     🎌 Breaking 1 Order: {breaking1_order}
    #     🎌 Breaking 2 Order: {breaking2_order}
    #     🎌 Just In Order: {just_in_order}
    #     🎌 CP Order: {cp_order}
    #     🎌 Tab 2 Order: {tab2_order}
    #     🎌 Tab 3 Order: {tab3_order}
    #     🎌 Tab 4 Order: {tab4_order}
    #     🎌 Tab 5 Order: {tab5_order}
    #     🎌 Tab 6 Order: {tab6_order}
    #     🎌 Top Headlines Order: {top_headline_order}
    #       """
    # )

    # # Print out all the headlines
    # print(
    #     f"""
    #     🔗 Breaking 1 URL: {breaking1_url}
    #     🔗 Breaking 2 URL: {breaking2_url}
    #     🔗 Just In URL: {just_in_url}
    #     🔗 CP URL: {cp_url}
    #     🔗 Tab 2 URL: {tab2_url}
    #     🔗 Tab 3 URL: {tab3_url}
    #     🔗 Tab 4 URL: {tab4_url}
    #     🔗 Tab 5 URL: {tab5_url}
    #     🔗 Tab 6 URL: {tab6_url}
    #     🔗 Top 1 URL: {top1_url}
    #     🔗 Top 2 URL: {top2_url}
    #     🔗 Top 3 URL: {top3_url}
    #     🔗 Top 4 URL: {top4_url}
    #     🔗 Top 5 URL: {top5_url}
    #     """
    # )


def get_connnecticut_insider_headlines():
    """
    This function scrapes the Connecticut Insider homepage
    """
    market_url = "https://www.ctinsider.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="

    # Get the HTML content of the Houston Chronicle homepage
    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all("div", class_="centerpiece-tab--main-headline")

    # We extract the text from the headlines and strip the whitespace.
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_url = remove_duplicate_prefix(
        market_url + cp_headlines[0].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    tab2_url = remove_duplicate_prefix(
        market_url + cp_headlines[1].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    tab3_url = remove_duplicate_prefix(
        market_url + cp_headlines[2].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    tab4_url = remove_duplicate_prefix(
        market_url + cp_headlines[3].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    tab5_url = remove_duplicate_prefix(
        market_url + cp_headlines[4].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    tab6_url = remove_duplicate_prefix(
        market_url + cp_headlines[5].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_centerpiece_tab"
    # Example: class="hide-rss-link hdnce-e hdnce-collection-105803-dynamic_centerpiece_tab"
    cp_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_centerpiece_tab")
    )

    cp_tab_id_list = []
    for tab in cp_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        cp_tab_id_list.append(f"{wcm_url}{tab_id}")

    (
        cp_order,
        tab2_order,
        tab3_order,
        tab4_order,
        tab5_order,
        tab6_order,
    ) = cp_tab_id_list

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = remove_duplicate_prefix(
            market + breaking_headlines[0]["href"], market_url
        )
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = remove_duplicate_prefix(
            market_url + breaking_headlines[1]["href"], market_url
        )
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
    except:
        just_in_headline = None
        just_in_url = None

    trending_now_bar = soup.find("section", class_="fourPack-breaking")

    # Find all the headlines in the trending now bar
    trending_headlines = trending_now_bar.find_all(
        "a", class_="fourPack--item-headline"
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    # We extract the text from the trending now headlines and strip the whitespace.
    try:
        trending1_headline = trending_headlines[0].text.strip()
        trending1_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[0]["href"].replace("?src=ctipromostrip", ""),
            market_url,
        )
    except:
        trending1_headline = None
        trending1_url = None
    try:
        trending2_headline = trending_headlines[1].text.strip()
        trending2_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[1]["href"].replace("?src=ctipromostrip", ""),
            market_url,
        )
    except:
        trending2_headline = None
        trending2_url = None
    try:
        trending3_headline = trending_headlines[2].text.strip()
        trending3_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[2]["href"].replace("?src=ctipromostrip", ""),
            market_url,
        )
    except:
        trending3_headline = None
        trending3_url = None
    try:
        trending4_headline = trending_headlines[3].text.strip()
        trending4_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[2]["href"].replace("?src=ctipromostrip", ""),
            market_url,
        )
    except:
        trending4_headline = None
        trending4_url = None
    try:
        # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
        # Example: class="hdnce-collection-105799-dynamic_headline_list"
        trending_order = soup.find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_four_pack")
        )

        top_headline_id = re.search(r"\d+", trending_order["class"][2]).group()

        trending_order = f"{wcm_url}{top_headline_id}"
    except:
        trending_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines list and extract the text from the headlines, stripping the whitespace.
    top_headlines_list = soup.find("ul", class_="coreHeadlineList--items")

    headline_list = top_headlines_list.find_all(
        "div", class_="coreHeadlineList--item-headline"
    )

    top1 = headline_list[0].text.strip()
    top2 = headline_list[1].text.strip()
    top3 = headline_list[2].text.strip()
    top4 = headline_list[3].text.strip()
    top5 = headline_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url + headline_list[0].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    top2_url = remove_duplicate_prefix(
        market_url + headline_list[1].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    top3_url = remove_duplicate_prefix(
        market_url + headline_list[2].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    top4_url = remove_duplicate_prefix(
        market_url + headline_list[3].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )
    top5_url = remove_duplicate_prefix(
        market_url + headline_list[4].find("a")["href"].replace("?src=ctipdensecp", ""),
        market_url,
    )

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_headline_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "Trending 1": trending1_headline,
            "Trending 2": trending2_headline,
            "Trending 3": trending3_headline,
            "Trending 4": trending4_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "Trending 1": trending1_url,
            "Trending 2": trending2_url,
            "Trending 3": trending3_url,
            "Trending 4": trending4_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "Trending 1": trending_order,
            "Trending 2": trending_order,
            "Trending 3": trending_order,
            "Trending 4": trending_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df


def get_connnecticut_post_headlines():
    """
    This function scrapes the Connecticut post homepage
    """
    market_url = "https://www.ctpost.com"
    wcm_url = "https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id="

    # Get the HTML content of the Houston Chronicle homepage
    soup = getSoup(market_url)

    ## --- CENTERPIECE HEADLINES --- ##

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all("div", class_="centerpiece-tab--main-headline")

    # We extract the text from the headlines and strip the whitespace.
    cp_headline = cp_headlines[0].text.strip()
    tab2_headline = cp_headlines[1].text.strip()
    tab3_headline = cp_headlines[2].text.strip()
    tab4_headline = cp_headlines[3].text.strip()
    tab5_headline = cp_headlines[4].text.strip()
    tab6_headline = cp_headlines[5].text.strip()

    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
    cp_url = remove_duplicate_prefix(
        market_url + cp_headlines[0].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    tab2_url = remove_duplicate_prefix(
        market_url + cp_headlines[1].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    tab3_url = remove_duplicate_prefix(
        market_url + cp_headlines[2].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    tab4_url = remove_duplicate_prefix(
        market_url + cp_headlines[3].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    tab5_url = remove_duplicate_prefix(
        market_url + cp_headlines[4].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )

    tab6_url = remove_duplicate_prefix(
        market_url + cp_headlines[5].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_centerpiece_tab"
    # Example: class="hide-rss-link hdnce-e hdnce-collection-105803-dynamic_centerpiece_tab"
    cp_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_centerpiece_tab")
    )

    cp_tab_id_list = []
    for tab in cp_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        cp_tab_id_list.append(f"{wcm_url}{tab_id}")

    (
        cp_order,
        tab2_order,
        tab3_order,
        tab4_order,
        tab5_order,
        tab6_order,
    ) = cp_tab_id_list

    ## --- BREAKING NEWS --- ##

    # Find all the headlines in the breaking news bar
    breaking_headlines = soup.find_all("a", class_="breakingNow--item-headline")

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_headline = breaking_headlines[0].text.strip()
        breaking1_url = remove_duplicate_prefix(
            market_url + breaking_headlines[0]["href"], market_url
        )
    except:
        breaking1_headline = None
        breaking1_url = None
    try:
        breaking2_headline = breaking_headlines[1].text.strip()
        breaking2_url = remove_duplicate_prefix(
            market_url + breaking_headlines[1]["href"], market_url
        )
    except:
        breaking2_headline = None
        breaking2_url = None
    try:
        just_in_headline = soup.find(
            "a", {"class": "justNow--item-headline"}
        ).text.strip()

        just_in_url = (
            f'{market_url}{soup.find("a", {"class": "justNow--item-headline"})["href"]}'
        )
    except:
        just_in_headline = None
        just_in_url = None

    trending_now_bar = soup.find("section", class_="fourPack-breaking")

    # Find all the headlines in the trending now bar
    trending_headlines = trending_now_bar.find_all(
        "a", class_="fourPack--item-headline"
    )

    # Find all the divs on the page that have a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now_tab"
    # Example: class="hdnce-collection-114722-dynamic_breaking_now_tab"
    breaking_tabs = soup.find_all(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now_tab")
    )

    breaking_tab_id_list = []
    for tab in breaking_tabs:
        # Get the string of digits in the collection
        # Example input: hdnce-collection-111490-dynamic_centerpiece_tab
        # Example output: 111490
        tab_id = re.search(r"\d+", tab["class"][2]).group()
        breaking_tab_id_list.append(f"{wcm_url}{tab_id}")

    try:
        breaking1_order = breaking_tab_id_list[0]
    except:
        breaking1_order = ""
    try:
        breaking2_order = breaking_tab_id_list[1]
    except:
        breaking2_order = ""

    # Find the div on the page that has a class that contains the strings "hdnce-collection-" AND "-dynamic_breaking_now" that resides inside of a div with an id of "zoneAL"
    # Example: class="hdnce-collection-107158-dynamic_breaking_now"
    try:
        just_in_order = soup.find("div", id="zoneAL").find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_breaking_now")
        )
        just_in_id = re.search(r"\d+", just_in_order["class"][2]).group()

        just_in_order = f"{wcm_url}{just_in_id}"
    except:
        just_in_order = ""

    # We extract the text from the trending now headlines and strip the whitespace.
    try:
        trending1_headline = trending_headlines[0].text.strip()
        trending1_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[0]["href"].replace("?src=rdctppromostrip", ""),
            market_url,
        )
    except:
        trending1_headline = None
        trending1_url = None
    try:
        trending2_headline = trending_headlines[1].text.strip()
        trending2_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[1]["href"].replace("?src=rdctppromostrip", ""),
            market_url,
        )
    except:
        trending2_headline = None
        trending2_url = None
    try:
        trending3_headline = trending_headlines[2].text.strip()
        trending3_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[2]["href"].replace("?src=rdctppromostrip", ""),
            market_url,
        )
    except:
        trending3_headline = None
        trending3_url = None
    try:
        trending4_headline = trending_headlines[3].text.strip()
        trending4_url = remove_duplicate_prefix(
            market_url
            + trending_headlines[3]["href"].replace("?src=rdctppromostrip", ""),
            market_url,
        )
    except:
        trending4_headline = None
        trending4_url = None
    try:
        # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
        # Example: class="hdnce-collection-105799-dynamic_headline_list"
        trending_order = soup.find(
            "div", class_=re.compile("hdnce-collection-.*-dynamic_four_pack")
        )

        top_headline_id = re.search(r"\d+", trending_order["class"][2]).group()

        trending_order = f"{wcm_url}{top_headline_id}"
    except:
        trending_order = ""

    ## --- TOP HEADLINES --- ##

    # Find the top headlines list and extract the text from the headlines, stripping the whitespace.
    top_headlines_list = soup.find("ul", class_="coreHeadlineList--items")

    headline_list = top_headlines_list.find_all(
        "div", class_="coreHeadlineList--item-headline"
    )

    top1 = headline_list[0].text.strip()
    top2 = headline_list[1].text.strip()
    top3 = headline_list[2].text.strip()
    top4 = headline_list[3].text.strip()
    top5 = headline_list[4].text.strip()

    top1_url = remove_duplicate_prefix(
        market_url
        + headline_list[0].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    top2_url = remove_duplicate_prefix(
        market_url
        + headline_list[1].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    top3_url = remove_duplicate_prefix(
        market_url
        + headline_list[2].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    top4_url = remove_duplicate_prefix(
        market_url
        + headline_list[3].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )
    top5_url = remove_duplicate_prefix(
        market_url
        + headline_list[4].find("a")["href"].replace("?src=rdctpdensecp", ""),
        market_url,
    )

    # Find the div on the page that has a class that contains the strings hdnce-collection" AND "-dynamic_headline_list"
    # Example: class="hdnce-collection-105799-dynamic_headline_list"
    top_headline_order = soup.find(
        "div", class_=re.compile("hdnce-collection-.*-dynamic_headline_list")
    )

    top_headline_id = re.search(r"\d+", top_headline_order["class"][2]).group()

    top_headline_order = f"{wcm_url}{top_headline_id}"

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%Y-%m-%d"
    )

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(markets["San Antonio"]["timezone"])).strftime(
        "%-I:%M %p"
    )

    # Create a dataframe out of the centerpiece headlines and the top headlines.
    latest_headlines_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_headline,
            "Breaking 2": breaking2_headline,
            "Just In": just_in_headline,
            "Trending 1": trending1_headline,
            "Trending 2": trending2_headline,
            "Trending 3": trending3_headline,
            "Trending 4": trending4_headline,
            "CP": cp_headline,
            "Tab 2": tab2_headline,
            "Tab 3": tab3_headline,
            "Tab 4": tab4_headline,
            "Tab 5": tab5_headline,
            "Tab 6": tab6_headline,
            "Top 1": top1,
            "Top 2": top2,
            "Top 3": top3,
            "Top 4": top4,
            "Top 5": top5,
        },
        index=[0],
    )

    latest_urls_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_url,
            "Breaking 2": breaking2_url,
            "Just In": just_in_url,
            "Trending 1": trending1_url,
            "Trending 2": trending2_url,
            "Trending 3": trending3_url,
            "Trending 4": trending4_url,
            "CP": cp_url,
            "Tab 2": tab2_url,
            "Tab 3": tab3_url,
            "Tab 4": tab4_url,
            "Tab 5": tab5_url,
            "Tab 6": tab6_url,
            "Top 1": top1_url,
            "Top 2": top2_url,
            "Top 3": top3_url,
            "Top 4": top4_url,
            "Top 5": top5_url,
        },
        index=[0],
    )

    latest_tab_order_df = pd.DataFrame(
        {
            "Date": date,
            "Time": time,
            "Breaking 1": breaking1_order,
            "Breaking 2": breaking2_order,
            "Just In": just_in_order,
            "Trending 1": trending_order,
            "Trending 2": trending_order,
            "Trending 3": trending_order,
            "Trending 4": trending_order,
            "CP": cp_order,
            "Tab 2": tab2_order,
            "Tab 3": tab3_order,
            "Tab 4": tab4_order,
            "Tab 5": tab5_order,
            "Tab 6": tab6_order,
            "Top 1": top_headline_order,
            "Top 2": top_headline_order,
            "Top 3": top_headline_order,
            "Top 4": top_headline_order,
            "Top 5": top_headline_order,
        },
        index=[0],
    )

    return latest_headlines_df, latest_urls_df, latest_tab_order_df


def handle_spreadsheet_update(
    latest_headlines_df, latest_urls_df, latest_tab_order_df, market
):
    """
    This function handles updating the market's spreadsheet with the new data.
    """
    market_spreadsheet_url = markets[market]["spreadsheet"]

    # Open the spreadsheet by its URL using gspread
    sh = gc.open_by_url(market_spreadsheet_url)

    # In one go, I want to store the first, second and third sheets in the spreadsheet in three separate dataframes
    # The names of the sheets are "Headline log", "URL log" and "Tab order log"
    historic_headline_log_df, historic_url_log_df, historic_tab_url_log_df = (
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    # We loop through the sheets in the spreadsheet
    for sheet in sh.worksheets():
        # If the sheet is called "Headline log"
        if sheet.title == "Headline log":
            # We store the sheet in the historic_headline_log_df dataframe
            historic_headline_log_df = pd.DataFrame(sheet.get_all_records())
        # If the sheet is called "URL log"
        elif sheet.title == "URL log":
            # We store the sheet in the historic_url_log_df dataframe
            historic_url_log_df = pd.DataFrame(sheet.get_all_records())
        # If the sheet is called "Tab order log"
        elif sheet.title == "Tab order log":
            # We store the sheet in the historic_tab_url_log_df dataframe
            historic_tab_url_log_df = pd.DataFrame(sheet.get_all_records())

    # Concatenate the latest dataframes with the historic dataframes.
    # The latest dataframes are first in the list so they will be on top
    # The historic dataframes are second in the list so they will be on the bottom
    updated_headline_log_df = pd.concat([latest_headlines_df, historic_headline_log_df])

    updated_url_log_df = pd.concat([latest_urls_df, historic_url_log_df])

    updated_tab_url_log_df = pd.concat([latest_tab_order_df, historic_tab_url_log_df])

    print("Setting the headline log")
    # Now we write the dataframes to the spreadsheet using the set_with_dataframe method
    # set_with_dataframe(
    #     sh.worksheet("Headline log"), updated_headline_log_df, include_index=False
    # )

    # print("Setting the URL log")
    # set_with_dataframe(sh.worksheet("URL log"), updated_url_log_df, include_index=False)

    # print("Setting the tab order log")
    # set_with_dataframe(
    #     sh.worksheet("Tab order log"), updated_tab_url_log_df, include_index=False
    # )
    api_call_handler(
        set_with_dataframe(
            sh.worksheet("Headline log"), updated_headline_log_df, include_index=False
        )
    )

    print("Setting the URL log")
    api_call_handler(
        set_with_dataframe(
            sh.worksheet("URL log"), updated_url_log_df, include_index=False
        )
    )

    print("Setting the tab order log")
    api_call_handler(
        set_with_dataframe(
            sh.worksheet("Tab order log"), updated_tab_url_log_df, include_index=False
        )
    )


# We loop through the markets dictionary
for market, info in markets.items():
    print(f"🏙️ Logging headlines for {market}...")
    if market == "San Antonio":
        print("--- San Antonio Express-News ---")
        print("📰 Scraping homepage...")
        # We call the function that scrapes the San Antonio Express-News homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_san_antonio_headlines()

        # api_call_handler(
        #     handle_spreadsheet_update(
        #         latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        #     )
        # )
        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )

    elif market == "Houston":
        print("--- Houston Chronicle ---")
        # We call the function that scrapes the Houston Chronicle homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_houston_headlines()

        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )

    elif market == "Albany":
        print("--- Albany Times Union ---")
        # We call the function that scrapes the Albany Times Union homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_albany_headlines()

        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )
    elif market == "San Francisco":
        print("--- San Francisco Chronicle ---")
        # We call the function that scrapes the San Francisco Chronicle homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_san_francisco_headlines()

        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )

    elif market == "Connecticut Insider":
        print("--- Connecticut Insider ---")
        # We call the function that scrapes the Connecticut Insider homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_connnecticut_insider_headlines()

        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )
    elif market == "Connecticut Post":
        print("--- Connecticut Post ---")
        # We call the function that scrapes the Connecticut Post homepage
        (
            latest_headlines_df,
            latest_urls_df,
            latest_tab_order_df,
        ) = get_connnecticut_post_headlines()

        handle_spreadsheet_update(
            latest_headlines_df, latest_urls_df, latest_tab_order_df, market
        )

    time.sleep(30)

# Remove the temporary json file. We don't anyone to see our service account credentials!
os.remove("service_account.json")
