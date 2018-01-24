import praw
# import threading
import time
import re
from urllib import parse
import psycopg2
from psycopg2 import sql
import logging
import os
from collections import OrderedDict

# constants
MAX_REPLIES = 5
THEME_NAMES = {
    'adventure': 'Adventure',
    'sci': 'Sci-Fi',
    'mystical': 'Mystical',
    'fantasy': 'Fantasy',
    'neutral': 'Neutral'
}
THEME_COLORS = {
    'Adventure': '/adv',
    'Sci-Fi': '/sci',
    'Mystical': '/mys',
    'Fantasy': '/fan',
    'Neutral': '/neu'
}
THEME_ICONS = {
    'Adventure': '/ico-adv',
    'Sci-Fi': '/ico-sci',
    'Mystical': '/ico-mys',
    'Fantasy': '/ico-fan',
    'Neutral': '/ico-neu'
}
CLASS_NAMES = {
    'tank': 'Tank',
    'melee': 'Fighter',
    'assassin': 'Assassin',
    'ranged': 'Ranged',
    'artillery': 'Ranged',
    'totem': 'Totem',
    'spell': 'Spell'
}

# get abs path
path = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(path, 'data', 'sppd.log')

# create logger
logger = logging.getLogger('sppd_bot')
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler(log_path)
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

# reddit
if 'REDDIT_USERNAME' in os.environ:
    reddit = praw.Reddit(client_id=os.environ['REDDIT_CLIENT_ID'],
        client_secret=os.environ['REDDIT_CLIENT_SECRET'], user_agent=os.environ['REDDIT_USER_AGENT'],
        username=os.environ['REDDIT_USERNAME'], password=os.environ['REDDIT_PASSWORD'])
else:
    reddit = praw.Reddit('bot1')

# subreddit = reddit.subreddit('SouthParkPhone')
subreddit = reddit.subreddit('EpicVTestSub')

conn = None
cursor = None
CARD_DATA = {}

# class streamThread (threading.Thread):
#     def __init__(self, threadID, table):
#         threading.Thread.__init__(self)
#         self.threadID = threadID
#         self.table = table
#     def run(self):
#         process_stream(self.table)

def main():
    initialize()
    # thread1 = streamThread(1, 'submissions')
    # thread2 = streamThread(2, 'comments')

    # thread1.start()
    # thread2.start()
    # thread1.join()
    # thread2.join()
    process_stream('submissions')
    process_stream('comments')
    conn.close()

def initialize():
    # create submissions and comments tables
    global conn, cursor, CARD_DATA
    if 'DATABASE_URL' in os.environ:
        parse.uses_netloc.append('postgres')
        url = parse.urlparse(os.environ['DATABASE_URL'])
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
    else:
        conn = psycopg2.connect(
            database='sppd_db',
            user='postgres',
            password='postgres',
            host='localhost',
            port='5432'
        )
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS submissions (id TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS comments (id TEXT)')
    conn.commit()

    # read data from database
    cursor.execute('SELECT * FROM cards')
    for row in cursor.fetchall():
        CARD_DATA[row[0]] = {
            'name': row[1],
            'theme': row[2],
            'type': row[3],
            'class': row[4],
            'rarity': row[5],
            'cost': row[6]
        }

def process_stream(table):
    logger.info('Start checking ' + table)
    print('Start checking', table)

    if table == 'submissions':
        # for submission in subreddit.stream.submissions():
        for submission in subreddit.new(limit=100):
            process_post(submission)

    elif table == 'comments':
        # for comment in subreddit.stream.comments():
        for comment in subreddit.comments(limit=100):
            process_post(comment)

    logger.info('Finish checking ' + table)
    print('Finish checking', table)

def process_post(post):
    if post.author == reddit.user.me():
        return

    if isinstance(post, praw.models.Submission):
        body = post.selftext
        table = 'submissions'
    elif isinstance(post, praw.models.Comment):
        body = post.body
        table = 'comments'
    else:
        return

    cursor.execute(sql.SQL('SELECT * FROM {} WHERE id = %s')
        .format(sql.Identifier(table)),
    [post.id])
    if cursor.fetchone() is None:
        matches = re.findall(r'(//southparkphone.gg/builder/#/([0-9,]+))', body, re.IGNORECASE)
        matches = OrderedDict((x, True) for x in matches).keys()
        reply = ''

        if len(matches) > 0:
            logger.info('Found decks in ' + table[:-1] + ' ' + post.id)
            print('Found decks in', table[:-1], post.id)

        for match_index, match in enumerate(matches):
            reply += prepare_reply(match_index + 1, match, len(matches))

        if reply:
            try:
                post.reply(reply)
                cursor.execute(sql.SQL('INSERT INTO {} VALUES (%s)')
                    .format(sql.Identifier(table)),
                [post.id])
                conn.commit()
                logger.info('Replied to ' + table[:-1] + ' ' + post.id)
                print('Replied to', table[:-1], post.id)
            except:
                logger.exception('Unable to reply ' + table[:-1] + ' ' + post.id)
                print('Unable to reply', table[:-1], post.id)
                time.sleep(30)
                process_stream(post, table)

            cursor.execute(sql.SQL('SELECT * FROM {}').format(sql.Identifier(table)))
            if len(cursor.fetchall()) > MAX_REPLIES:
                cursor.execute(sql.SQL('DELETE FROM {0} WHERE CTID IN '
                    '(SELECT CTID from {1} limit %s)')
                    .format(sql.Identifier(table), sql.Identifier(table)),
                [str(int(MAX_REPLIES / 10))])
                conn.commit()
                logger.debug('Cleared some rows in table ' + table)
                # print('Cleared some rows in table', table)

def prepare_reply(index, value, count):
    logger.info('Deck #' + str(index) + ' - ' + value[1])
    print('Deck #', str(index), '-', value[1])
    reply = '[Deck #' + str(index) + '](' + value[0] + ')  \n'
    deck = get_deck_info(value[1])

    if len(deck['errors']) > 0:
        reply += generate_error_mesage(deck['errors'])

    reply += generate_deck_summary(deck['themes'], deck['cost'])
    reply += generate_card_list(deck['cards'])

    if index < count:
        reply += '\n***\n\n'

    logger.debug('Reply content is ready')
    # print('Reply content is ready')
    return reply

def get_deck_info(deck):
    total_count = 0
    total_cost = 0
    cards = OrderedDict([('Tank', []), ('Fighter', []), ('Assassin', []), ('Ranged', []), ('Totem', []), ('Spell', []), ('Unknown', [])])
    unique_ids = []
    themes = []
    theme_count = 0
    errors = []

    card_ids = list(map(int, deck.split(',')))
    for card_id in card_ids:
        if card_id in CARD_DATA:
            total_count += 1
            total_cost += CARD_DATA[card_id]['cost']

            class_name = get_class_name(CARD_DATA[card_id]['class'])
            cards[class_name].append(CARD_DATA[card_id])

            if card_id not in unique_ids:
                unique_ids.append(card_id)
                logger.debug('new card found: ' + str(card_id))
                # print('new card found:', str(card_id))
            else:
                errors.append('duplicate card ' + str(card_id))
                logger.debug('duplicate card found: ' + str(card_id))
                # print('duplicate card found:', str(card_id))

            theme_name = get_theme_name(CARD_DATA[card_id]['theme'])
            if theme_name not in themes and theme_name != 'Unknown':
                themes.append(theme_name)
                if theme_name != 'Neutral':
                    theme_count += 1
                    logger.debug('new theme found: ' + theme_name)
                    # print('new theme found:', theme_name)
        else:
            cards['Unknown'].append({'name': str(card_id), 'theme': 'unknown'})
            logger.debug('unknow card found: ' + str(card_id))
            # print('unknow card found:', str(card_id))

    if len(card_ids) < 12:
        errors.append('not enough cards')
    elif len(card_ids) > 12:
        errors.append('too many cards')

    if theme_count > 2:
        errors.append('too many themes')

    return {
        'themes': themes,
        'cost': format(total_cost / total_count, '.1f'),
        'cards': cards,
        'errors': errors
    }

def get_class_name(name):
    if name in CLASS_NAMES:
        return CLASS_NAMES[name]
    else:
        return 'Unknown'

def get_theme_name(name):
    if name in THEME_NAMES:
        return THEME_NAMES[name]
    else:
        return 'Unknown'

def generate_error_mesage(errors):
    message = '*Invalid deck: '
    logger.debug('Preparing error message')
    # print('Preparing error message')

    for i, error in enumerate(errors):
        message += error
        if i >= len(errors) - 1:
            message += '*  \n'
        else:
            message += '; '

    logger.debug('Error message is ready')
    # print('Error message is ready')
    return message

def generate_deck_summary(themes, cost):
    summary = '**Themes:** '
    logger.debug('Preparing deck summary')
    # print('Preparing deck summary')

    for i, theme in enumerate(themes):
        if theme != 'Neutral':
            summary += '[](' + THEME_ICONS[theme] + ') '

    summary += ('&nbsp;' * 10) + '**Avg. Cost: ' + cost + '**  \n\n'

    logger.debug('Deck summary is ready')
    # print('Deck summary is ready')
    return summary

def generate_card_list(cards):
    card_list = ''
    logger.debug('Preparing card list')
    # print('Preparing card list')

    for card_class, card in cards.items():
        if len(card) <= 0:
            continue

        card_list += '* **' + card_class + ':** '

        for i, c in enumerate(card):
            theme_name = get_theme_name(c['theme'])
            if theme_name == 'Unknown':
                card_list += c['name']
            else:
                card_list += '[' + c['name'] + '](' + THEME_COLORS[theme_name] + ')'

            if i < len(card) - 1:
                card_list += ', '
            else:
                card_list += '  \n'

    logger.debug('Card list is ready')
    # print('Card list is ready')
    return card_list


if __name__ == '__main__':
    main()
