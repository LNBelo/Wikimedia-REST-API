# -*- coding: utf-8 -*-

"""
Script for get data in Wikimedia REST_API
Insert categories in categories.txt
"""

import time
import json
import requests
import multiprocessing
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import quote


def get_files_in_category(category):
    def categorymembers(cmcontinue):
        s = requests.Session()
        url = "https://commons.wikimedia.org/w/api.php"

        params = {
            "action": "query",
            "generator": "categorymembers",
            "gcmtitle": f"Category:{category}",
            "gcmlimit": "500",
            "gcmcontinue": cmcontinue,
            "prop": "imageinfo",
            "iiprop": "url",
            "format": "json"
        }

        r = s.get(url=url, params=params)
        payload = r.json()
        pages = payload['query']['pages']
        dic["pages"].update(pages)

        return payload

    dic = {"data": {"category": category}, "pages": {}}

    # build dic
    gcmcontinue = ''
    while True:
        payload_temp = categorymembers(gcmcontinue)
        if 'continue' in payload_temp:
            gcmcontinue = payload_temp['continue']['gcmcontinue']
        else:
            break

    # remove subcategories
    for key in list(dic["pages"].keys()):
        # ns is namespace, for File ns = 6
        if dic["pages"][key]["ns"] != 6:
            del dic["pages"][key]

    # total files in category
    n = 0
    pageids = dic['pages']
    for pageid in pageids:
        pageid = dic['pages'][pageid]
        if 'imageinfo' in pageid:
            n += 1

    dic["data"]["total_files"] = n

    return dic


def get_metrics_from_file(result_dict, lst_urls, start, end,
                          referer="all-referers", agent="all-agents", granularity="daily"):
    i = 1
    for url_file in lst_urls:
        now = datetime.now().strftime("%Hh%Mm%Ss")
        print(f'\r\tDone {i / len(lst_urls) * 100:.2f} %. Current time: {now}', end='')

        file_path = url_file.replace('https://upload.wikimedia.org', '')
        file_path = quote(file_path, safe='')
        s = requests.Session()
        url = f"https://wikimedia.org/api/rest_v1/metrics/mediarequests/per-file/{referer}/{agent}/{file_path}" \
              f"/{granularity}/{start}/{end}"

        headers = {'User-Agent': 'Lucas Belo/1.0 (lucas.belo@wmnobrasil.org; lucasnascimentobelo@gmail.com)'
                                 'requests/Python 3.9'}

        r = s.get(url, headers=headers)
        payload = r.json()

        result_dict[url_file] = payload

        i += 1


def most_viewed_media(dic):
    sorted_pages = sorted(dic['pages'].values(), key=lambda x: x['total_file_views'], reverse=True)

    title_1 = sorted_pages[0]['title']
    views_title_1 = sorted_pages[0]['total_file_views']
    title_2 = sorted_pages[1]['title']
    views_title_2 = sorted_pages[1]['total_file_views']

    dic['data']['most_viewed_files'] = {1: {'title': title_1, 'total_file_views': views_title_1},
                                        2: {'title': title_2, 'total_file_views': views_title_2}}


def chunked(dic, num_sublists):
    urls = []

    for page_id, page_data in dic["pages"].items():
        urls.extend([dic["pages"][page_id]["imageinfo"][0]["url"]])

    total = len(urls)
    section = total // num_sublists

    sublists = []
    start = 0

    for i in range(num_sublists):
        if i == num_sublists - 1:
            sublist = urls[start: total]
        else:
            sublist = urls[start: (start + section)]
        sublists.append(sublist)
        start += section

    return sublists


def requests_per_second_rest_api():
    total_requests = 23

    category = 'Wiki Movimento Brasil Upload of the Museu Histórico Nacional Collection'
    dic = get_files_in_category(category)
    start = '2024010100'
    end = '2024013100'

    urls = []
    for page_id, page_data in dic["pages"].items():
        if len(urls) <= total_requests:
            urls.extend([dic["pages"][page_id]["imageinfo"][0]["url"]])
        else:
            break

    start_timer = time.perf_counter()

    get_metrics_from_file(dic, urls, start, end)

    stop_timer = time.perf_counter()
    total_time = stop_timer - start_timer

    print(f"\n\n\tTotal requests: {total_requests}")
    print(f"\tTotal time: {total_time:.2f}s")
    print(f"\tTotal requests per second: {total_requests / total_time:.2f}")
    print(
        f"\tDo up to {100 / (total_requests / total_time):.0f}"
        f" simultaneous processes to not exceed 100 requests per second")


# falta fazer uma verificação quando a API REST retornar json vazio
def verification(dic, category, start, end):
    def categoryinfo():
        s = requests.Session()
        url = "https://commons.wikimedia.org/w/api.php"

        params = {
            "action": "query",
            "prop": "categoryinfo",
            "titles": f"Category:{category}",
            "format": "json"
        }

        r = s.get(url=url, params=params)
        payload = r.json()

        files_in_category = 0
        for page_id, page_info in payload['query']['pages'].items():
            files_in_category = page_info['categoryinfo']['files']

        return files_in_category

    def mediaviews(file):
        s = requests.Session()
        url = f"https://pageviews.wmflabs.org/mediaviews"

        params = {
            "project": "commons.wikimedia.org",
            "files": file,
            "start": start,
            "end": end,
            "referer": "all-referers",
            "agent": "all-agents",
            "autolog": 'false',
            "mutevalidations": 'true'
        }

        r = s.get(url=url, params=params)
        soup = BeautifulSoup(r.content, 'html.parser')
        soup = soup.find('span', {'class': "pull-right"})

        return soup

    start = datetime.strptime(start, '%Y%m%d%H').strftime('%Y-%m-%d')
    end = datetime.strptime(end, '%Y%m%d%H').strftime('%Y-%m-%d')

    total_files = dic["data"]["total_files"]
    files = categoryinfo()
    if total_files != files:
        print('The number of files in the category is different from that obtained')

    file_1 = dic['data']['most_viewed_files']["1"]["title"].replace('File:', '')
    views_file_1 = mediaviews(file_1)
    file_2 = dic['data']['most_viewed_files']["2"]["title"].replace('File:', '')
    views_file_2 = mediaviews(file_2)


def main():
    start = '2024010100'
    end = '2024013100'

    with open("categories.txt") as categories:
        lst_categories = categories.readlines()

    i = 1
    for category in lst_categories:
        category = category.strip()
        print(f"{i}/{len(lst_categories)}. cat: {category}")

        print('\tGetting the category files...')
        dic = get_files_in_category(category)

        # split urls into groups
        n_process = 20  # 20
        pages = chunked(dic, n_process)

        # manage dictionary for each process
        manager = multiprocessing.Manager()
        result_dict = manager.dict()

        # Create n simultaneous processes
        process = [multiprocessing.Process(target=get_metrics_from_file, args=(result_dict, page, start, end))
                   for page in pages]

        for p in process:
            p.start()

        for p in process:
            p.join()

        total_views = 0

        for page_id, page in dic['pages'].items():
            url_file = page['imageinfo'][0]['url']
            dic['pages'][page_id]['views'] = result_dict[url_file]

            file_views = 0
            if 'items' in result_dict[url_file]:
                items = result_dict[url_file]['items']
                for item in items:
                    file_views += item['requests']

            dic['pages'][page_id]['total_file_views'] = file_views
            total_views += file_views

        dic['data']['start'] = start
        dic['data']['end'] = end
        dic['data']['total_views'] = total_views

        most_viewed_media(dic)

        # verification(dic, category, start, end)

        with open(f'{category}.json', "w") as file:
            json.dump(dic, file)


if __name__ == '__main__':
    # start_timer = time.perf_counter()

    main()

    # stop_timer = time.perf_counter()
    # total_time = stop_timer - start_timer
    # print(f"\n\nTotal time: {total_time:.2f}s")

    # start = '2024010100'
    # end = '2024013100'
    #
    # category = "Wiki Movement Brasil's initiative with Museu do Colono"
    # with open(f"{category}.json") as file:
    #     dic = json.load(file)
    #
    # verification(dic, category, start, end)
