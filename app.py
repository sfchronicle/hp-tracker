import os
from datetime import datetime

import feedparser
import gspread
import pytz

SERVICE_ACCOUNT = os.environ.get('SERVICE_ACCOUNT')

# Create a temporary json file based on the SERVICE_ACCOUNT env variable
with open('service_account.json', 'w') as f:
    f.write(SERVICE_ACCOUNT)

def track_san_antonio():
    # The first step is to collect HP items
    cp = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-1-NEW-111490.php').entries[0].title
    tab2 = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-2-105801.php').entries[0].title
    tab3 = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-3-105803.php').entries[0].title
    tab4 = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-4-105802.php').entries[0].title
    tab5 = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-5-105804.php').entries[0].title
    tab6 = feedparser.parse('https://www.expressnews.com/default/collectionRss/SAEN-Homepage-Centerpiece-Tab-6-105805.php').entries[0].title
    try:
        breaking1 = feedparser.parse('https://www.expressnews.com/news/local/collectionRss/Breaking-Tab-1-114722.php').entries[0].title
    except:
        breaking1 = None
    try:
        breaking2 = feedparser.parse('https://www.expressnews.com/news/local/collectionRss/Breaking-Tab-2-115061.php').entries[0].title
    except:
        breaking2 = None

    # The second step is to authenticate with Google Sheets
    sa = gspread.service_account(filename='service_account.json')
    
    # The third step is to open the spreadsheet we want to write to
    sh = sa.open('EN HP log')

    # The fourth step is to open the worksheet we want to write to
    wks = sh.worksheet('HP Log')

    # Step 5: Insert a row at the top of the worksheet
    wks.insert_row([],2)

    # Step 6: Update the cells in the row we just inserted
    wks.update_cell(2, 1, datetime.now(pytz.timezone('US/Central')).strftime('%-I:%M %p %Y-%m-%d'))
    wks.update_cell(2, 2, breaking1)
    wks.update_cell(2, 3, breaking2)
    wks.update_cell(2, 4, cp)
    wks.update_cell(2, 5, tab2)
    wks.update_cell(2, 6, tab3)
    wks.update_cell(2, 7, tab4)
    wks.update_cell(2, 8, tab5)
    wks.update_cell(2, 9, tab6)

def track_houston():
    cp = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-6-101001.php').entries[0].title
    tab2 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-5-101000.php').entries[0].title
    tab3 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-1-103009.php').entries[0].title
    tab4 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-3-98479.php').entries[0].title
    tab5 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-4-98480.php').entries[0].title
    tab6 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/PHD-HC-Main-Centerpiece-Tab-2-98476.php').entries[0].title
    try:
        breaking1 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/Breaking-News-Tab-1-112576.php').entries[0].title
    except:
        breaking1 = None
    try:
        breaking2 = feedparser.parse('https://www.houstonchronicle.com/default/collectionRss/Breaking-News-Tab-2-112578.php').entries[0].title
    except:
        breaking2 = None


    sa = gspread.service_account(filename='service_account.json')
    sh = sa.open('HC HP log')

    wks = sh.worksheet('HP Log')
    wks.insert_row([],2)

    wks.update_cell(2, 1, datetime.now(pytz.timezone('US/Central')).strftime('%-I:%M %p %Y-%m-%d'))
    wks.update_cell(2, 2, breaking1)
    wks.update_cell(2, 3, breaking2)
    wks.update_cell(2, 4, cp)
    wks.update_cell(2, 5, tab2)
    wks.update_cell(2, 6, tab3)
    wks.update_cell(2, 7, tab4)
    wks.update_cell(2, 8, tab5)
    wks.update_cell(2, 9, tab6)

try:
    track_san_antonio()
except:
    pass

try:
    track_houston()
except:
    pass

# Remove the temporary json file
os.remove('service_account.json')