from src import api
from src import esearch
import os
import sys
import getopt


if __name__ == '__main__':
    app_key = os.getenv('APP_KEY')
    if not app_key:
        raise Exception('APP_KEY not found')

    page_size = None
    page = api.DEFAULT_PAGE
    output = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], longopts=['page_size=', 'num_pages=', 'output='], shortopts='')
    except getopt.GetoptError:
        raise Exception('Parameter parsing error')

    for opt, arg in opts:
        if opt == '--page_size':
            page_size = arg
        elif opt == '--num_pages':
            page = arg
        elif opt == '--output':
            output = arg

    if not page_size:
        raise Exception('The required parameter page_size is missing')

    data = api.get_data(app_key, page_size=page_size, page=page)

    for item in data:
        print(item)
    if output:
        api.file_storage(data, output)

    data = api.format_data(data)

    # elasticsearch
    esearch.es_storage(data)
