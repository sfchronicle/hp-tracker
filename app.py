import os
import re
import time
from datetime import datetime

import gspread
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

# We grab our service account from a Github secret
SERVICE_ACCOUNT = os.environ.get('SERVICE_ACCOUNT')

# We create a dictionary of the markets we want to track.
markets = {
    'San Antonio': {
        'url': 'https://www.expressnews.com',
        'timezone': 'US/Central',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/1F073i7iMDEU0q2B8K3nG881YDr-f1bCXzc2h24V_dOs/edit#gid=0',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    },
    'Houston': {
        'url': 'https://www.houstonchronicle.com',
        'timezone': 'US/Central',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/19IZkVDucvXYT2EyHQ-8Yu2MSrknYK3Co_HI3TAd_hBA/edit#gid=0',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    },
    'San Francisco': {
        'url': 'https://www.sfchronicle.com',
        'timezone': 'US/Pacific',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/1YhvmHOeT5RQLmoef6zZhWfwLD87JIAFiGqzS_llbTu8/edit#gid=0',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    },
    'Albany': {
        'url': 'https://www.timesunion.com',
        'timezone': 'US/Eastern',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/1NREkjXsMslgsl_8XaS-9W_3t3gU67jfmptoN9-9yt1k/edit#gid=1676782739',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    },
    'Connecticut Insider': {
        'url': 'https://www.ctinsider.com',
        'timezone': 'US/Eastern',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/10V626AzMp1NXaW4wOnUq_VArl79XpCdbJQkbIk9esGA/edit#gid=964675505',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    },
    'Connecticut Post': {
        'url': 'https://www.ctpost.com',
        'timezone': 'US/Eastern',
        'spreadsheet': 'https://docs.google.com/spreadsheets/d/1wMvD70EZO27TyzFY80cOxZPdFTCSqnB4YMarwuf-1vI/edit#gid=0',
        'Headline log worksheet': 'Headline log',
        'URL log worksheet': 'URL log',
        'Tab order log worksheet': 'Tab order log'
    }
}

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open('service_account.json', 'w') as f:
    f.write(SERVICE_ACCOUNT)

# We authenticate with Google using the service account json we created earlier.
sa = gspread.service_account(filename='service_account.json')

def remove_duplicate_prefix(url, prefix):
    if url.startswith(prefix * 2):
        url = url.replace(prefix, "", 1)
    return url

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

# Now we're going to scrape the URLs for the headlines we just scraped.
def get_urls(market_url):
    """
    This function takes a URL and returns the URLs for the headlines from the homepage.
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
        cp_url = f'{market_url}{cp_headlines[0]["href"]}'
        tab2_url = f'{market_url}{cp_headlines[1]["href"]}'
        tab3_url = f'{market_url}{cp_headlines[2]["href"]}'
        tab4_url = f'{market_url}{cp_headlines[3]["href"]}'
        tab5_url = f'{market_url}{cp_headlines[4]["href"]}'
        tab6_url = f'{market_url}{cp_headlines[5]["href"]}'
    else:
    # We extract the URLs from the headlines and strip the whitespace. The href is in the a tag, so we need to use a for loop to extract the URLs.
        cp_url = f'{market_url}{cp_headlines[0].find("a")["href"]}'
        tab2_url = f'{market_url}{cp_headlines[1].find("a")["href"]}'
        tab3_url = f'{market_url}{cp_headlines[2].find("a")["href"]}'
        tab4_url = f'{market_url}{cp_headlines[3].find("a")["href"]}'
        tab5_url = f'{market_url}{cp_headlines[4].find("a")["href"]}'
        tab6_url = f'{market_url}{cp_headlines[5].find("a")["href"]}'

    # Use remove_duplicate_prefix to remove the duplicate prefix from the URLs for each.
    cp_url = remove_duplicate_prefix(cp_url, market_url)
    tab2_url = remove_duplicate_prefix(tab2_url, market_url)
    tab3_url = remove_duplicate_prefix(tab3_url, market_url)
    tab4_url = remove_duplicate_prefix(tab4_url, market_url)
    tab5_url = remove_duplicate_prefix(tab5_url, market_url)
    tab6_url = remove_duplicate_prefix(tab6_url, market_url)

    # We extract the URLs from the breaking news headlines and strip the whitespace. There isn't always a breaking news bar, so we use a try/except block to handle the error.
    try:
        breaking1_url = f'{market_url}{breaking_headlines[0]["href"]}'
    except:
        breaking1_url = None
    try:
        breaking2_url = f'{market_url}{breaking_headlines[1]["href"]}'
    except:
        breaking2_url = None

    # Finally, we return the URLs so that we can hand them off to the next function.
    return breaking1_url, breaking2_url, cp_url, tab2_url, tab3_url, tab4_url, tab5_url, tab6_url

def get_tab_order(market_url):
    '''
    This function takes a market URL and returns a list of tab names in order.
    '''

    # We use the getSoup function to get a BeautifulSoup object
    soup = getSoup(market_url)

    if market_url == 'https://www.timesunion.com':
        # Set the values of cp, tab2, tab3, tab4, tab5, tab6 to https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614 each
        cp = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'
        tab2 = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'
        tab3 = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'
        tab4 = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'
        tab5 = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'
        tab6 = 'https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id=116614'

    else:

        # # I want the section element with the class of 'centerpiece' that contains at least one div element in it.
        centerpiece_section = soup.find_all('section', class_='centerpiece')

        # Loop through the centerpiece_section list. If the section does not contain a div element, remove it from the list.
        for section in centerpiece_section:
            if not section.find('div'):
                centerpiece_section.remove(section)

        centerpiece_section = centerpiece_section[0]

        # In centerpiece_section, find all div elements that has a class that contains "hdnce-collection-"
        tabs = centerpiece_section.find_all('div', class_=lambda x: x and 'hdnce-collection-' in x)

        # Loop through the tabs list
        for tab in tabs:
            for tab_class in tab['class']:
                if 'dynamic_headline_list' in tab_class:
                    tabs.remove(tab)

        # Loop through the tabs list and get the class attribute of each div element.
        tab_classes = []
        for tab in tabs:
            # Loop through the class attribute of each div element and get the class that contains "hdnce-collection-"
            for tab_class in tab['class']:
                # Use regex to store the string of digits that is at least four characters long in tab_wcm_id
                tab_wcm_id = re.findall(r'\d{4,}', tab_class)
                if tab_wcm_id:
                    tab_classes.append(tab_wcm_id)

        # Flatten the tab_classes list
        tab_classes = [item for sublist in tab_classes for item in sublist]

        # In a list comprehension, loop through the tab_classes list and prepend them with https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id={}
        tab_classes = ['https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id={}'.format(tab_class) for tab_class in tab_classes]

        cp, tab2, tab3, tab4, tab5, tab6 = tab_classes
    try:
        breaking_headline_bar = soup.find('div', class_='breakingNow--list')

        # Find the divs with a class that contains "hdnce-collection-"
        breaking_headline_bar = breaking_headline_bar.find_all('div', class_=lambda x: x and 'hdnce-collection-' in x)

        breaking_classes = []

        for breaking_tab in breaking_headline_bar:
            for breaking_class in breaking_tab['class']:
                breaking_wcm_id = re.findall(r'\d{4,}', breaking_class)
                if breaking_wcm_id:
                    breaking_classes.append(breaking_wcm_id)

        breaking_classes = [item for sublist in breaking_classes for item in sublist]

        # In a list comprehension, loop through the breaking_classes list and prepend them with https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id={}
        breaking_classes = ['https://wcm.hearstnp.com/index.php?_wcmAction=business/collection&id={}'.format(breaking_class) for breaking_class in breaking_classes]

        try:
            breaking1 = breaking_classes[0]
        except:
            breaking1 = ''
        try:
            breaking2 = breaking_classes[1]
        except:
            breaking2 = ''
    except:
        breaking1 = ''
        breaking2 = ''

    return breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6

def record_tab_order(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6):
    """
    This function takes a spreadsheet name, worksheet name, timezone, and tabs and writes them to a Google Sheet.
    """

    # We direct the bot to the worksheet we want to write to.
    wks = spreadsheet.worksheet(worksheet)

    # We add in a new row at the top of the spreadsheet.
    wks.insert_row([],2)

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d')

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(timezone)).strftime('%-I:%M %p')

    # Create a pandas dataframe with the headlines
    df = pd.DataFrame({'Date':date, 'Time':time, 'Breaking 1':breaking1, 'Breaking 2':breaking2, 'CP':cp, 'Tab 2':tab2, 'Tab 3':tab3, 'Tab 4':tab4, 'Tab 5':tab5, 'Tab 6':tab6}, index=[0])

    wks.update([df.columns.values.tolist()] + df.values.tolist())

def record_headlines(spreadsheet, worksheet, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6):
    """
    This function takes a spreadsheet name, worksheet name, timezone, and headlines and writes them to a Google Sheet.
    """

    # We direct the bot to the worksheet we want to write to.
    wks = spreadsheet.worksheet(worksheet)

    # We add in a new row at the top of the spreadsheet.
    wks.insert_row([],2)

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d')

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(timezone)).strftime('%-I:%M %p')

    # Create a pandas dataframe with the headlines
    df = pd.DataFrame({'Date': date, 'Time': time, 'Breaking 1': breaking1, 'Breaking 2': breaking2, 'CP': cp, 'Tab 2': tab2, 'Tab 3': tab3, 'Tab 4': tab4, 'Tab 5': tab5, 'Tab 6': tab6}, index=[0])

    wks.update([df.columns.values.tolist()] + df.values.tolist())

def record_urls(spreadsheet, worksheet, timezone, breaking1_url, breaking2_url, cp_url, tab2_url, tab3_url, tab4_url, tab5_url, tab6_url):
    """
    This function takes a spreadsheet name, worksheet name, timezone, and URLs and writes them to a Google Sheet.
    """

    # We direct the bot to the worksheet we want to write to.
    wks = spreadsheet.worksheet(worksheet)

    # We add in a new row at the top of the spreadsheet.
    wks.insert_row([],2)

    # Create a new variable called date and store the current date in the following format: YYYY-MM-DD
    date = datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%d')

    # Create a new variable called time and store the current time in the appropriate timezone in a 12-hour format without a leading zero
    time = datetime.now(pytz.timezone(timezone)).strftime('%-I:%M %p')

    # Create a pandas dataframe with the URLs
    df = pd.DataFrame({'Date': [date], 'Time': [time], 'Breaking 1': [breaking1_url], 'Breaking 2': [breaking2_url], 'CP': [cp_url], 'Tab 2': [tab2_url], 'Tab 3': [tab3_url], 'Tab 4': [tab4_url], 'Tab 5': [tab5_url], 'Tab 6': [tab6_url]})

    # Do what you did above but with a dataframe in one go using a batch update. Don't include the headers.
    wks.update([df.columns.values.tolist()] + df.values.tolist())

# We loop through the markets dictionary
for market, info in markets.items():
    print(f'🏙️ Logging headlines for {market}...')
    try:
        # Get the URL for the market
        market_url = info['url']

        # Get the headlines for the market
        breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6 = get_headlines(market_url)

        # Get the URLs for the market
        breaking1_url, breaking2_url, cp_url, tab2_url, tab3_url, tab4_url, tab5_url, tab6_url = get_urls(market_url)

        # Get the tab order for the market
        breaking1_order, breaking2_order, cp_order, tab2_order, tab3_order, tab4_order, tab5_order, tab6_order = get_tab_order(market_url)

        # Get the spreadsheet and worksheet names
        spreadsheet = sa.open_by_url(info['spreadsheet'])
        headline_ws = info['Headline log worksheet']
        url_ws = info['URL log worksheet']
        tab_order_ws = info['Tab order log worksheet']

        # Get the timezone
        timezone = info['timezone']

        # Record the headlines
        record_headlines(spreadsheet, headline_ws, timezone, breaking1, breaking2, cp, tab2, tab3, tab4, tab5, tab6)

        # Record the URLs
        record_urls(spreadsheet, url_ws, timezone, breaking1_url, breaking2_url, cp_url, tab2_url, tab3_url, tab4_url, tab5_url, tab6_url)

        # Record the tab order
        record_tab_order(spreadsheet, tab_order_ws, timezone, breaking1_order, breaking2_order, cp_order, tab2_order, tab3_order, tab4_order, tab5_order, tab6_order)

    except Exception as e:
        print(f'Error: {e}')
        pass
    time.sleep(10)

# Remove the temporary json file. We don't anyone to see our service account credentials!
os.remove('service_account.json')