![Hearst home page tracker banner](img/banner.png)

Hey y'all!

This bot has one simple task: Log what headlines are where on Hearst newspaper home pages every hour.

## How it works

Every hour I have a Github Action run `app.py`, which goes out to handful of Hearst newspaper sites to scrape the headlines that are present on their home pages. It then opens an existing Google Sheet tied to that market and adds the headlines. That's it!

## Hearst newspapers currently tracked

- [San Antonio Express-News](https://www.expressnews.com/)
- [Houston Chronicle](https://www.houstonchronicle.com/)
- [San Francisco Chronicle](https://www.sfchronicle.com/)
- [Albany Times Union](https://www.timesunion.com/)
- [Connecticut Post](https://www.ctpost.com/)
- [Connecticut Insider](https://www.ctinsider.com/)