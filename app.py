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

    # Step 7: Check for repeating rows
    # For each column in the spreadsheet, I want to see if there are five or more consecutive rows that have the same value. if so, I want to highlight those rows in yellow using wks.format()
    for col in range(1, 10):
        for row in range(2, 100):
            if wks.cell(row, col).value == wks.cell(row + 1, col).value == wks.cell(row + 2, col).value == wks.cell(row + 3, col).value == wks.cell(row + 4, col).value:
                wks.format(f'{row}:{col}', {'backgroundColor': {'red': 1, 'green': 1, 'blue': 0}})


try:
    track_san_antonio()
except:
    pass

# Remove the temporary json file
os.remove('service_account.json')