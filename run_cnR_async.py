# coding: utf-8

from cnR_async.util.workers import main
from cnR_async.constants import OUTPUT_PATH

import argparse
import asyncio
import logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--page_count', type=int, default=30,
        help='define number of pages to be scraped, default 30' 
    )
    parser.add_argument(
        '-d', '--delay', type=float, default=2.0,
        help='define delay between each request, default 2 secs'
    )
    parser.add_argument(
        '--downloader_count', type=int, default=3,
        help='define the number of download tasks that will be run ' \
             'concurrently, default 4'
    )
    parser.add_argument(
        '-o', '--output_path', default=OUTPUT_PATH,
        help='define the path to store output txt files, default ' \
              '".\cnReuters_output"'
    )
    parser.add_argument(
        '-t', '--tag', action='store_true',
        help='if present, tagged result will be output, each tag a file'
    )
    parser.add_argument(
        '-c', '--con_file', default='tag_configure.json',
        help='define which json file to be used for tag operation, default' \
             '"tag_configure.json"'
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='if present, decrease logging level to logging.DEBUG; default ' \
             'level is logging.INFO'
    )

    args = parser.parse_args()

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    asyncio.run(
        main(
            args.page_count,      
            args.delay,           
            args.downloader_count,
            args.output_path,     
            args.tag,             
            args.con_file,        
            level
        )
    )
