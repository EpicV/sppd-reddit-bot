# import sqlite3
import os
from urllib import parse
import psycopg2
import logging
import json

# get abs path
path = os.path.dirname(os.path.abspath(__file__))
# db_path = os.path.join(path, 'data', 'db.sqlite3')
log_path = os.path.join(path, 'data', 'sppd.log')
card_json = os.path.join(path, 'data', 'cards.json')

# create logger
logger = logging.getLogger('update_cards')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler(log_path)
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

# connect database
# conn = sqlite3.connect(db_path)
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
c = conn.cursor()

# create table
c.execute('''CREATE TABLE IF NOT EXISTS cards
    (id INTEGER, name TEXT, theme TEXT, type TEXT, class TEXT,
    rarity TEXT, cost INTEGER)''')

# read json
with open(card_json) as json_file:
    CARD_DATA = json.load(json_file)[0]

# prepare statement to find non existing cards
nec_query = 'SELECT id FROM cards WHERE id NOT IN ('

# loop through card data
for key, card in CARD_DATA.items():
    c.execute('SELECT * FROM cards WHERE id = ?', (key,))
    data = c.fetchone()

    # add new card
    if data is None:
        c.execute('INSERT INTO cards VALUES (?, ?, ?, ?, ?, ?, ?)',
            (key, card['name'], card['theme'], card['type'], card['class'],
                card['rarity'], card['cost'],))
        logger.info('Added card: ' + key + ' - ' + card['name'])
        print('Added card:', key, '-', card['name'])

    # update card
    elif (card['name'] != data[1] or card['theme'] != data[2]
            or card['type'] != data[3] or card['class'] != data[4]
            or card['rarity'] != data[5] or int(card['cost']) != data[6]):
        c.execute('''UPDATE cards
            SET name = ?, theme = ?, type = ?, class = ?, rarity = ?, cost = ?
            WHERE id = ?''', (card['name'], card['theme'], card['type'],
                card['class'], card['rarity'], card['cost'], key,))
        logger.info('Updated card: ' + key + ' - ' + card['name'])
        print('Updated card:', key, '-', card['name'])

    # update statement
    nec_query += key + ', '

# find non existing records
c.execute(nec_query[:-2] + ')')
nec = c.fetchall()
if len(nec) > 0:
    ids = ', '.join(map(str,list(zip(*nec))[0]))
    c.execute('DELETE FROM cards WHERE id IN (' + ids + ')')
    logger.info('Deleted cards: ' + ids)
    print('Deleted cards:', ids)

# commit changes
logger.info('Committing changes...')
print('Committing changes...')
conn.commit()

# close connection
logger.info('Done! Closing connection...')
print('Done! Closing connection...')
conn.close()
