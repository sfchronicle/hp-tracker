import os
import time
from datetime import datetime

import gspread
import pytz
import requests
from bs4 import BeautifulSoup

# We grab our service account from a Github secret
SERVICE_ACCOUNT = os.environ.get('SERVICE_ACCOUNT')

# We create a dictionary of the markets we want to track.
markets = {
    'San Antonio': {
        'url': 'https://www.expressnews.com/',
        'timezone': 'US/Central',
        'spreadsheet': 'EN HP log',
        'worksheet': 'HP Log'
    },
    'Houston': {
        'url': 'https://www.houstonchronicle.com/',
        'timezone': 'US/Central',
        'spreadsheet': 'HC HP log',
        'worksheet': 'HP Log'
    },
    'San Francisco': {
        'url': 'https://www.sfchronicle.com/',
        'timezone': 'US/Pacific',
        'spreadsheet': 'SF HP log',
        'worksheet': 'HP Log'
    },
    'Albany': {
        'url': 'https://www.timesunion.com/',
        'timezone': 'US/Eastern',
        'spreadsheet': 'TU HP log',
        'worksheet': 'HP Log'
    },
    'Connecticut Insider': {
        'url': 'https://www.ctinsider.com/',
        'timezone': 'US/Eastern',
        'spreadsheet': 'CT Insider HP log',
        'worksheet': 'HP Log'
    },
    'Connecticut Post': {
        'url': 'https://www.ctpost.com/',
        'timezone': 'US/Eastern',
        'spreadsheet': 'CT Post HP log',
        'worksheet': 'HP Log'
    }
}

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open('service_account.json', 'w') as f:
    f.write(SERVICE_ACCOUNT)

def getSoup(url):
    """
    This function takes a URL and returns a BeautifulSoup object.
    """
    page = requests.get(url) 
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup

def get_headlines(market_url):
    """
    This function takes a URL and returns the headlines from the homepage.
    """
    
    # We use the getSoup function to get a BeautifulSoup object
    soup = getSoup(market_url)

    # Find all the headlines in the breaking news bar.
    breaking_headlines = soup.find_all('a', class_='breakingNow--item-headline')

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all('div', class_='centerpiece-tab--main-headline')

    # Certain markets, like the Albany Times Union, use a different template for their homepage. In this scenario, we need to use a different class to find the headlines.
    if cp_headlines == []:
        cp_headlines = soup.find_all('a', class_='dynamicSpotlight--item-header')

    # We extract the text from the headlines and strip the whitespace.
    cp = cp_headlines[0].text.strip()
    tab2 = cp_headlines[1].text.strip()
    tab3 = cp_headlines[2].text.strip()
    tab4 = cp_headlines[3].text.strip()
    tab5 = cp_headlines[4].text.strip()
    tab6 = cp_headlines[5].text.strip()

    # We extract the text from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1 = breaking_headlines[0].text.strip()
    except:
        breaking1 = None
    try:
        breaking2 = breaking_headlines[1].text.strip()
    except:
        breaking2 = None

    # Finally, we return the headlines so that we can hand them off to the next function.
    return breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6

def record_headlines(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6):
    """
    This function takes a spreadsheet name, worksheet name, timezone, and headlines and writes them to a Google Sheet.
    """
    
    # We authenticate with Google using the service account json we created earlier.
    sa = gspread.service_account(filename='service_account.json')
    
    # We direct the bot to the market spreadsheet we want to write to.
    sh = sa.open(spreadsheet)

    # We direct the bot to the worksheet we want to write to.
    wks = sh.worksheet(worksheet)

    # We add in a new row at the top of the spreadsheet.
    wks.insert_row([],2)

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d')

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(timezone)).strftime('%-I:%M %p')

    # We write the date, time, and headlines to the spreadsheet.
    wks.update_cell(2, 1, date)
    wks.update_cell(2, 2, time)
    wks.update_cell(2, 3, breaking1)
    wks.update_cell(2, 4, breaking2)
    wks.update_cell(2, 5, cp)
    wks.update_cell(2, 6, tab2)
    wks.update_cell(2, 7, tab3)
    wks.update_cell(2, 8, tab4)
    wks.update_cell(2, 9, tab5)
    wks.update_cell(2, 10, tab6)

# We loop through the markets dictionary
for market, info in markets.items():
    print(f'🐝 Logging headlines for {market}...')
    try:
        # Get the URL for the market
        market_url = info['url']

        # Get the headlines for the market
        breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6 = get_headlines(market_url)

        # Get the spreadsheet and worksheet names
        spreadsheet = info['spreadsheet']
        worksheet = info['worksheet']

        # Get the timezone
        timezone = info['timezone']

        # Record the headlines
        record_headlines(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6)

    except Exception as e:
        print(f'Error: {e}')
        pass
    time.sleep(10)

# Remove the temporary json file. We don't anyone to see our service account credentials!
os.remove('service_account.json')