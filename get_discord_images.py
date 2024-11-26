'''
TODO:
    * Add in a refresher that grabs any dead cdn links
'''
import re 
from urllib import parse
import requests
import json
import pandas as pd

with open('secrets.json', 'r') as f:
    secrets = json.loads(f.read())
DISCORD_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, REFRESH_TOKEN = tuple(e for e in secrets.values())
with open('constants.json', 'r') as f:
    const = json.loads(f.read())
COL_NAME_MAP, SHEET_ID, SHEET_NAME, PARSED_LINKS = tuple(e for e in const.values())

def get_sheets_headers():
    url = "https://accounts.google.com/o/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }
    headers = {
        "content-type": "application/x-www-form-urlencoded"
    }

    r = requests.request("POST", url, data=data, headers=headers)
    data = r.json()
    token = data["access_token"]
    token_type = data["token_type"]
    headers = {
            "Authorization": f'{token_type} {token}'
        }
    return headers


def get_sheets_data(_range='A:F'):
    '''_range is in A1 notation (i.e. A:I gives all rows for columns A to I)'''
    headers = get_sheets_headers()
    sheet_link = f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{SHEET_NAME}!{_range}'
    r = requests.get(sheet_link, headers=headers)
    values = r.json()['values']
    df = pd.DataFrame(values[1:])
    df.columns = values[0]
    df = df.apply(lambda x: x.str.strip())
    return df


def clean_sheets_data(df):
    df.columns = [COL_NAME_MAP.get(e, e) for e in df.columns]
    df.time = pd.to_datetime(df.time)
    df = df[['time', 'username', 'form_link']]
    df = df[~pd.isna(df['form_link'])]
    return df


def get_unparsed_links(new_data):
    def check_parsed(new_link, parsed_links):
        parsed = True if new_link in parsed_links else False
        return parsed

    try:
        parsed_links = pd.read_csv(PARSED_LINKS, parse_dates=['time'])
    except FileNotFoundError:
        parsed_links = pd.DataFrame({'time': [], 'username': [], 'form_link': [], 'img_link': []})
    
    unique_parsed_links = list(parsed_links.form_link.unique())
    new_data['parsed'] = new_data.form_link.apply(lambda x: check_parsed(x, unique_parsed_links))
    new_data = new_data[~(new_data.parsed)]
    return new_data, parsed_links


def get_msg_parts(msg_link):
    is_cdn_link = 'attachments' in msg_link
    split_locs = (-2, -1) if not is_cdn_link else (-3, -2)
    _split = msg_link.split('/')
    channel, msg = tuple(_split[split_loc] for split_loc in split_locs)
    return channel, msg


def get_discord_ids(links):
    '''Get all links beginning with https; no media and no cdn links!'''
    pat = '(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])'
    parsed_links = re.findall(pat, links)
    friendly_links = [get_msg_parts(e[2]) for e in parsed_links]
    return friendly_links


def retrieve_message(channel_id, message_id):
    headers = {
        'Authorization': f'Bot {DISCORD_TOKEN}'
    }
    req_link = f'https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}'
    r = requests.get(req_link, headers=headers)
    r = json.loads(r.text)
    return r


def retrieve_attachment_links(_json):
    attachments = _json['attachments']
    return [att['url'] for att in attachments]


def get_attachments(channel_id, message_id):
    message = retrieve_message(channel_id, message_id)
    attachments = retrieve_attachment_links(message)
    return attachments


def get_all_attachments(ids):
    all_attachments = [get_attachments(id[0], id[1]) for id in ids]
    all_attachments = [y for x in all_attachments for y in x]
    return all_attachments


def main():
    new_data = get_sheets_data()
    new_data = clean_sheets_data(new_data)
    new_data, parsed_data = get_unparsed_links(new_data)
    if len(new_data) > 0:
        new_data['msg_links'] = new_data.form_link.apply(get_discord_ids)
        new_data['img_links'] = new_data.msg_links.apply(get_all_attachments)
        new_data = new_data.explode('img_links')[['time', 'username', 'form_link', 'img_links']]
        new_data = new_data.rename({'img_links': 'img_link'}, axis=1)
        parsed_data = pd.concat([parsed_data, new_data])
        parsed_data.to_csv(PARSED_LINKS, index=False)


if __name__ == '__main__':
    main()