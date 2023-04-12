import os
from datetime import datetime

import gspread
import pytz
import requests
from bs4 import BeautifulSoup

SERVICE_ACCOUNT = os.environ.get('SERVICE_ACCOUNT')

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
    }
}

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open('service_account.json', 'w') as f:
    f.write(SERVICE_ACCOUNT)

def getSoup(url):
    page = requests.get(url) 
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup

def get_headlines(market_url):
    soup = getSoup(market_url)

    breaking_headlines = soup.find_all('a', class_='breakingNow--item-headline')

    # Find all the divs with a class of "centerpiece-tab--main-headline"
    cp_headlines = soup.find_all('div', class_='centerpiece-tab--main-headline')

    # Certain markets, like the Albany Times Union, use a different theme for their homepage. The headlines feature a different class.
    if cp_headlines == []:
        cp_headlines = soup.find_all('a', class_='dynamicSpotlight--item-header')

    cp = cp_headlines[0].text.strip()
    tab2 = cp_headlines[1].text.strip()
    tab3 = cp_headlines[2].text.strip()
    tab4 = cp_headlines[3].text.strip()
    tab5 = cp_headlines[4].text.strip()
    tab6 = cp_headlines[5].text.strip()
    try:
        breaking1 = breaking_headlines[0].text.strip()
    except:
        breaking1 = None
    try:
        breaking2 = breaking_headlines[1].text.strip()
    except:
        breaking2 = None

    return breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6

def record_headlines(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6):
    # The second step is to authenticate with Google Sheets
    sa = gspread.service_account(filename='service_account.json')
    
    # The third step is to open the spreadsheet we want to write to
    sh = sa.open(spreadsheet)

    # The fourth step is to open the worksheet we want to write to
    wks = sh.worksheet(worksheet)

    # Step 5: Insert a row at the top of the worksheet
    wks.insert_row([],2)

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d')

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(timezone)).strftime('%-I:%M %p')

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

# Loop through the markets dictionary
for market, info in markets.items():
    try:
        # Get the URL for the market
        market_url = info['url']
        # Get the headlines for the market
        breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6 = get_headlines(market_url)
        # Get the spreadsheet and worksheet names
        spreadsheet = info['spreadsheet']
        worksheet = info['worksheet']
        timezone = info['timezone']
        # Record the headlines
        record_headlines(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6)
    except Exception as e:
        print(f'Error: {e}')
        pass

# Remove the temporary json file
os.remove('service_account.json')