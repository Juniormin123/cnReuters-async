# coding: utf-8

from ..constants import MAIN_FMT, DATE_FMT, LOGGER_NAME

import logging
import sys
import os
import asyncio
import time
from typing import overload, Optional, Union, List, Dict

import aiohttp


# logging

def get_main_logger(base_name: str = LOGGER_NAME, 
                    level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(base_name)
    logger.setLevel(level)
    # only add handlers if there is none, prevent duplicate outputs
    if not logger.hasHandlers():
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt=MAIN_FMT, datefmt=DATE_FMT, style='{')
        )
        handler.setLevel(level)
        logger.addHandler(handler)
    return logger


def get_logger(name: str, base_name: str = LOGGER_NAME) -> logging.Logger:
    logger = logging.getLogger(f'{base_name}.{name}')
    return logger


# tools

def _replace_to_empty(s: str, *to_replace: str) -> str:
    """del multiple chars in a string"""
    for chars in to_replace:
        s = s.replace(chars, '')
    return s


def _replace(s: str) -> str:
    return _replace_to_empty(s, '\n', '\t')


def check_path(path: str, logger: logging.Logger) -> None:
    if path in os.listdir():
        logger.info(f'path {path} already exists')
    else:
        os.mkdir(os.path.join('.', path))
        logger.info(f"{os.path.join(os.path.abspath('.'), path)} created")


# base logger class for custom objects

class SetLogger:
    
    logger: logging.Logger
        
    @classmethod
    def set_logger(cls, name: str) -> None:
        cls.logger = get_logger(name)


# async request

async def async_get(url: str, 
                    msg: str, 
                    headers: dict, 
                    session: aiohttp.ClientSession, 
                    logger: logging.Logger,
                    *,
                    text: bool = True) -> str:
    t1 = time.time()
    returned: str
    try:
        async with session.get(url, headers=headers) as resp:
            if not text:
                # binary mode
                #returned = await resp.read()
                pass
            else:
                returned = await resp.text(encoding='utf-8')
            if resp.status >= 400:
                logger.warning(f'{msg} [{resp.status} {resp.reason} ' \
                               f'{time.time()-t1:.5f}s] ')
            else:
                logger.info(f'{msg} [{resp.status} {resp.reason} ' \
                            f'{time.time()-t1:.5f}s] ')
            return returned
    except aiohttp.ClientError as ce:
        logger.exception(ce)
        return ''
