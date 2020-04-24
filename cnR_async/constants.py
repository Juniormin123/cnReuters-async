# coding: utf-8

MAIN_FMT = '[{name}][{asctime} {msecs:0>7.3f}][{levelname}]' \
           '-pid:{process} th_id:{thread}- {message!s}'
DATE_FMT = '%y/%m/%d %H:%M:%S'
LOGGER_NAME = 'cnR_async'
OUTPUT_PATH = 'cnReuters_output'

HEADERS = {
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                  'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                  'Chrome/80.0.3987.163 Safari/537.36'
}
BASE_URL = 'https://cn.reuters.com/news/archive/' \
           'topic-cn-top-news?view=page&page={}&pageSize=10'

