import scrapy
import re
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

class MySpider(scrapy.Spider):
    name = 'myspider'
    start_urls = ['http://southparkphone.gg/']

    def parse(self, response):
        cards = {}

        for card in response.css('div.cards-listing__card'):
            card_id = 0
            card_name = card.css('div.card-preview__name-key::text').extract_first()
            card_cost = card.css('div.card-preview__mana-cost::text').extract_first()

            card_type = 'unknown'
            card_class = 'unknown'
            card_theme = 'unknown'
            card_rarity = 'unknown'
            css_class = card.css('div.card-preview::attr(class)').extract_first()

            re_type = re.search(r'card-preview--type-([a-zA-Z]+)', css_class, re.IGNORECASE)
            if re_type is not None:
                card_type = re_type.group(1)

            re_class = re.search(r'card-preview--class-type-([a-zA-Z]+)', css_class, re.IGNORECASE)
            if re_class is not None:
                card_class = re_class.group(1)

            re_theme = re.search(r'card-preview--theme-([a-zA-Z]+)', css_class, re.IGNORECASE)
            if re_theme is not None:
                card_theme = re_theme.group(1)

            re_rarity = re.search(r'card-preview--rarity-([a-zA-Z]+)', css_class, re.IGNORECASE)
            if re_rarity is not None:
                card_rarity = re_rarity.group(1)

            card_link = card.css('a.card-preview__link::attr(href)').extract_first()
            card_image = card.css('img.card-preview__image::attr(src)').extract_first()

            re_id = re.search(r'/cards/([0-9]+)-.*', card_link, re.IGNORECASE)
            if re_id is not None:
                card_id = re_id.group(1)

            cards[card_id] = {
                'name': card_name,
                'type': card_type,
                'class': card_class,
                'theme': card_theme,
                'rarity': card_rarity,
                'cost': card_cost,
                'link': card_link,
                'image': card_image,
            }

        yield cards

path = os.path.dirname(os.path.abspath(__file__))
card_json = os.path.join(path, 'data', 'cards.json')

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'FEED_FORMAT': 'json',
    'FEED_URI': 'file:///' + card_json
})

if os.path.exists(card_json):
    os.remove(card_json)

process.crawl(MySpider)
process.start()
