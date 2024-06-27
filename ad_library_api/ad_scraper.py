#Code by Roland Adorjani @RolandAdorjani
import time

from facebook import GraphAPIError

from fb_conn import graph, token
from pygments import highlight, lexers, formatters

import pandas as pd

from urllib.parse import urlsplit, parse_qs

import os

countries = [ "DE"
    # 'BR', 'IN', 'GB', 'US', 'CA', 'AR', 'AU', 'AT', 'BE', 'CL', 'CN', 'CO', 'HR', 'DK', 'DO',
    #          'EG', 'FI', 'FR', 'DE', 'GR', 'HK', 'ID', 'IE',
    #          'IL', 'IT', 'JP', 'JO', 'KW', 'LB', 'MY', 'MX', 'NL', 'NZ', 'NG', 'NO', 'PK', 'PA', 'PE', 'PH', 'PL',
    #          'RU', 'SA', 'RS', 'SG', 'ZA', 'KR', 'ES', 'SE', 'CH', 'TW', 'TH', 'TR', 'AE', 'VE', 'PT',
    #          'LU', 'BG', 'CZ', 'SI', 'IS', 'SK', 'LT', 'TT', 'BD', 'LK', 'KE', 'HU', 'MA', 'CY', 'JM', 'EC', 'RO', 'BO',
    #          'GT', 'CR', 'QA', 'SV', 'HN', 'NI', 'PY', 'UY', 'PR', 'BA', 'PS', 'TN', 'BH', 'VN', 'GH', 'MU', 'UA', 'MT',
    #          'BS', 'MV', 'OM', 'MK', 'LV', 'EE', 'IQ', 'DZ', 'AL', 'NP', 'MO', 'ME', 'SN', 'GE', 'BN',
    #          'UG', 'GP', 'BB', 'AZ', 'TZ', 'LY', 'MQ', 'CM', 'BW', 'ET', 'KZ', 'NA', 'MG', 'NC', 'MD', 'FJ', 'BY', 'JE',
    #          'GU', 'YE', 'ZM', 'IM', 'HT', 'KH', 'AW', 'PF', 'AF', 'BM', 'GY', 'AM', 'MW', 'AG', 'RW', 'GG', 'GM', 'FO',
    #          'LC', 'KY', 'BJ', 'AD', 'GD', 'VI', 'BZ', 'VC', 'MN',
    #          'MZ', 'ML', 'AO',
    #          'GF', 'UZ', 'DJ', 'BF', 'MC', 'TG', 'GL', 'GA', 'GI', 'CD', 'KG', 'PG', 'BT', 'KN', 'SZ', 'LS', 'LA', 'LI',
    #          'MP', 'SR', 'SC', 'VG', 'TC', 'DM', 'MR', 'AX', 'SM', 'SL', 'NE', 'CG', 'AI', 'YT', 'CV', 'GN', 'TM', 'BI',
    #          'TJ', 'VU', 'SB', 'ER', 'WS', 'AS', 'FK', 'GQ', 'TO', 'KM', 'PW', 'FM', 'CF', 'SO', 'MH',
    #          'VA', 'TD', 'KI', 'ST', 'TV', 'NR', 'RE', 'LR', 'ZW', 'CI', 'MM', 'AN', 'AQ', 'BQ', 'BV', 'IO', 'CX', 'CC',
    #          'CK', 'CW', 'TF', 'GW', 'HM', 'XK', 'MS', 'NU', 'NF', 'PN', 'BL', 'SH', "MF", "PM", "SX", "GS", "SS", "SJ",
    #          "TL", "TK", "UM", "WF", "EH"
]


# Four changes are neeeded to use this code:
# 1. Change 'countries = []' to ISO country code: https://www.nationsonline.org/oneworld/country_code_list.html
# 2. Change Ad Library Access Token (args["search_terms"]), available here:  https://developers.facebook.com/tools/explorer/
# 3. Change Ad Library (args["access_token"]) search term or else do full search with *
# 4. Change this to the folder on your computer where you want the scraped csv to appear

# formatted_json = ujson.dumps(r, sort_keys=True, indent=4)
# colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
# print(colorful_json)

def build_args(url=None):
    for k, v in args.items():
        args[k] = v

    if url:
        query = urlsplit(url).query
        params = parse_qs(query)
        for k, v in params.items():
            args[k] = v[0]

    return args


graph.version = "v20.0"

for country in countries:
    args = dict()
    args[
        "access_token"] = ''
    args["search_terms"] = '*'
    args["ad_type"] = 'ALL'
    args["ad_reached_countries"] = [country]
    args["media_type"] = ["MEME", "IMAGE"]
    # args["languages"] = ["en"]
    args['ad_active_status'] = 'ALL'
    args["ad_delivery_date_min"] = "2024-05-22"
    args["ad_delivery_date_max"] = "2024-06-22"
    args["fields"] = ("id,ad_creation_time,ad_creative_bodies,ad_creative_link_captions,ad_creative_link_descriptions,"
                      "ad_creative_link_titles,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,"
                      "age_country_gender_reach_breakdown,bylines,currency,delivery_by_region,"
                      "demographic_distribution,estimated_audience_size,eu_total_reach,impressions,languages,page_id,"
                      "page_name,publisher_platforms,InsightsRangeValue,target_ages,target_gender,target_locations")

    method = "/ads_archive"

    dfs = []
    # r = graph.request(method, args)
    # pages = graph.get_all_connections()
    while True:
        try:
            r = graph.request(method, args)
            break
        except GraphAPIError as g_a:
            if g_a.code in [613, 2]:
                print("first iteration", g_a.code)
                time.sleep(60)
                continue
            else:
                raise g_a
    dfs.append(pd.DataFrame(r.get('data', [])))

    i = 0
    next_page = r.get('paging', {}).get('next')
    print(next_page)
    while True:
        try:
            # get next page
            args = build_args(url=next_page)
            while True:
                try:
                    r = graph.request(method, args)
                    break
                except GraphAPIError as g_a:
                    if g_a.code in [613, 2]:
                        print("error code:", g_a.code)
                        time.sleep(10)
                        continue
                    else:
                        raise g_a
            next_page = r.get('paging', {}).get('next')
            if not next_page:
                break

            dfs.append(pd.DataFrame(r.get('data', [])))

            i += 1
            print("done", i)
            time.sleep(10)
        except Exception as e:
            raise e

    df = pd.concat(dfs)

    # df = pd.concat([df.drop(['impressions'], axis=1), df['impressions'].apply(pd.Series)], axis=1)
    # df.rename(columns={'lower_bound': 'lower_bound_impressions', 'upper_bound': 'upper_bound_impressions'}, inplace=True)

    df.to_csv('./path_to_file.csv'.format(country), encoding='utf8')
    time.sleep(10)
