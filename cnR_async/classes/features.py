# coding: utf-8

from ..util import funcs
from ..constants import BASE_URL, HEADERS

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
import aiohttp

class Entry(funcs.SetLogger):
    """represent one news entry in a news list page"""

    page: int
    index: int
    time: str
    date: str
    title: str
    content: str
    tag: str
    configure: dict = dict()
    
    def __init__(self, page: int, index: int, 
                 time_: str, date: str, title: str, content: str):
        self.page = page
        self.index = index
        self.time = time_
        self.date = date
        self.title = title
        self.content = content
        # default empty tag
        self.tag = ''

    def get_tag(self) -> None:
        tag_dict: Dict[str, List[str]]
        try:
            if self.configure:
                for tag_dict in self.configure['tag_conf']:
                    tag_name: str 
                    tag_words: List[str]
                    tag_name, tag_words = list(tag_dict.items())[0]
                    for word in tag_words:
                        if word in self.title:
                            self.tag = tag_name
                            break
                    if self.tag:
                        break
                else:
                    # if tag cannot be found, mark it as `untagged`
                    self.tag = 'untagged'
                self.logger.debug(f'Page {self.page} Entry {self.index} ' \
                                  f'{self.date} tagged "{self.tag}"')
        except Exception as e:
            # exception here will not terminate the program
            self.logger.exception(e)
    
    @classmethod
    def get_tag_configure(cls, conf_file: Optional[str]) -> None:
        if conf_file:
            with open(conf_file, 'r', encoding='utf-8') as conf:
                try:
                    cls.configure = json.loads(conf.read())
                    cls.logger.info(f'{conf_file} read for tag configure')
                except Exception as e:
                    cls.logger.exception(e)
                    #cls.configure = dict()
        else:
            cls.logger.warning('no tag configure file provided, no tag process')
    
    def __str__(self):
        s = f'Page {self.page} Entry {self.index}\n' \
            f'Tag: {self.tag}\n' \
            f'Date: {self.date} {self.time}\n' \
            f'{self.title}\n' \
            f'{self.content}\n'
        return s
    
    __repr__ = __str__


class Page(funcs.SetLogger):
    """represent an html page including a number of news entries"""
    
    page: int
    url: str
    unparsed_text: str
    #parsed_test: str
    entries: List[Entry]
    
    def __init__(self, page: int):
        self.page = page
        self.url = BASE_URL.format(page)
        self.entries: List[Entry] = []
    
    async def request(self, 
                      session: aiohttp.ClientSession, 
                      logger: Optional[logging.Logger] = None) -> None:
        if not logger:
            logger = self.logger
        self.unparsed_text = await funcs.async_get(
            self.url, f'Page({self.page})', HEADERS, session, logger, text=True
        )
    
    def extract(self) -> None:
        """fill self.entries with extracted Entry objects"""
        try:
            html = BeautifulSoup(self.unparsed_text, 'lxml')
            articles = html.find('section', class_='module-content') \
                           .find_all('article', class_='story')
            for i, atcl in enumerate(articles):
                try:
                    title = atcl.find('h3', class_='story-title').text
                    content = atcl.find('p').text
                    time_ = atcl.find('span', class_='timestamp').text
                    e = self._create_Entry(i, time_, title, content)
                    e.get_tag()
                    self.entries.append(e)
                except Exception as e:
                    # prevent terminating
                    self.logger.exception(e)
        except Exception as e:
            # page scale prevent terminating other tasks
            self.logger.exception(e)
            
    
    def _create_Entry(self, index: int, 
                      date_or_time: str, title: str, content: str) -> Entry:
        title = funcs._replace(title)
        content = funcs._replace(content)
        if ':' in date_or_time:
            time_ = date_or_time
        else:
            time_ = ''
            date = date_or_time
        # add today as date if time is present
        if time_:
            td = datetime.today()
            date = f'{td.year}年 {td.month}月 {td.day}日'
        return Entry(self.page, index, time_, date, title, content)
    
    def __str__(self):
        s = ''
        for entry in self.entries:
            s += str(entry) + '\n'
        return s


class Document(funcs.SetLogger):
    
    pages: List[Page]
    path: str
    _timestamp: str
    
    fmt = '%Y-%m-%d_%H-%M-%S'
    
    def __init__(self, path: str):
        self.pages: List[Page] = []
        self.path = path
        self._timestamp = ''
    
    def add(self, page: Page) -> None:
        self.pages.append(page)
    
    def output(self, tag: bool = True) -> None:
        # sort by Page.page
        self.pages.sort(key=lambda p: p.page)
        self._timestamp = datetime.now().strftime(self.fmt)
        
        self._output_total()
        if tag:
            self._output_tag()
    
    def _output_total(self) -> None:
        # use timestamp as file name
        f = os.path.join(self.path, self._timestamp+'.txt')
        with open(f, 'w', encoding='utf-8') as output:
            for p in self.pages:
                output.write(str(p))
        self.logger.info(f'total content output to {f}')
    
    def _output_tag(self) -> None:
        tag_entry_dict: Dict[str, str] = dict()
        # iterate through all Entry in all Page
        for p in self.pages:
            for e in p.entries:
                if e.tag in tag_entry_dict.keys():
                    tag_entry_dict[e.tag] += str(e) + '\n'
                else:
                    tag_entry_dict[e.tag] = str(e) + '\n'
        # one file for each tag
        for tag, entries_str in tag_entry_dict.items():
            f = os.path.join(self.path, f'{self._timestamp}_{tag}.txt')
            with open(f, 'a+', encoding='utf-8') as tag_output:
                tag_output.write(entries_str)
            self.logger.info(f'Tag {tag} output to {f}')
        self.logger.info(f'All tags output finished')
