# encoding: utf-8

from ..classes.features import Entry, Page, Document
from ..constants import OUTPUT_PATH, LOGGER_NAME
from .funcs import get_logger, get_main_logger, check_path

import logging
import time
import asyncio
from datetime import datetime
from typing import List, Optional

import aiohttp


def dispatch(page_count: int, 
             task_q: asyncio.Queue, 
             logger: logging.Logger) -> None:
    """create Page object and put it to an asyncio.Queue"""
    for i in range(page_count):
        p = Page(i)
        task_q.put_nowait(p)
    logger.info(f'{page_count} Page created')


async def downloader(index: int, 
                     task_q: asyncio.Queue,
                     output_q: asyncio.Queue,
                     session: aiohttp.ClientSession,
                     delay: float) -> None:
    """coroutine, get a Page from task queue, run Page.request() then put 
    it to an output queue"""
    t1 = time.time()
    logger = get_logger(f'downloader_{index}')
    logger.info(f'Worker{index} starts')
    while task_q.qsize() > 0:
        try:
            page: Page = await asyncio.wait_for(task_q.get(), timeout=0.5)
            await page.request(session, logger)
            output_q.put_nowait(page)
            task_q.task_done()
            await asyncio.sleep(delay)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            task_q.task_done()
            logger.exception(e)
    logger.info(f'Worker{index} finished, {time.time()-t1:.5f}s')


async def processor(index: int, 
                    output_q: asyncio.Queue, 
                    downloader_list: List[asyncio.Task],
                    document: Document) -> None:
    """
    coroutine, get a Page from output queue, run Page.extract();
    only exit if all downloader tasks are done, and the output queue is empty
    """
    t1 = time.time()
    logger = get_logger(f'processor_{index}')
    logger.info(f'Processor{index} starts')
    while True:
        try:
            # determine the exit condition before doing anything else
            condition = all([task.done() for task in downloader_list])
            if condition and output_q.qsize() == 0:
                break
            page: Page = await asyncio.wait_for(output_q.get(), 0.5)
            page.extract()
            document.add(page)
            # make sure to cede control back to the eventloop
            await asyncio.sleep(0.1)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.exception(e)
    logger.info(f'Processor{index} finished, {time.time()-t1:.5f}s')


async def main(page_count: int, 
               delay: float, 
               downloader_count: int, 
               output_path: Optional[str],
               tag: bool, 
               conf_file: Optional[str], 
               level: int) -> None:
    """
    coroutine, executes the following in order: check path, set class logger, 
    create queues, create and dispatch Pages, create ClientSession, create
    tasks to a list, await task queue, gather all tasks, Document.output()
    """
    logger = get_main_logger(base_name=LOGGER_NAME, level=level)
    t1 = time.time()
    
    # check output path
    if not output_path:
        output_path = OUTPUT_PATH
    check_path(output_path, logger)
    
    # set class logger 
    Entry.set_logger('Entry')
    Page.set_logger('Page')
    Document.set_logger('Document')
    
    # reset Entry tag configure
    Entry.get_tag_configure(conf_file)
    
    # generate queues
    task_q: asyncio.Queue = asyncio.Queue()
    output_q: asyncio.Queue = asyncio.Queue()
    
    # generate Pages
    dispatch(page_count, task_q, logger)
    
    # create client session
    s = aiohttp.ClientSession()
    
    # create downloader task
    downloader_list: List[asyncio.Task] = []
    try:
        for i in range(downloader_count):
            downloader_list.append(
                asyncio.create_task(
                    downloader(i, task_q, output_q, s, delay)
                )
            )
        # create processor task and global Document object
        doc = Document(output_path)
        processor_task = asyncio.create_task(
            processor(1, output_q, downloader_list, doc)
        )
        # wait for task queue to be emptied, all created tasks will be 
        # scheduled
        await task_q.join()
        
        # await until all tasks are done
        task_list = downloader_list + [processor_task]
        await asyncio.gather(*task_list)
        
        # output extracted pages and entries to file
        doc.output()
    finally:
        await s.close()
        logger.info('ClientSession closed.')
        logger.info(f'all done, total time {time.time()-t1:.5f}s')
        
