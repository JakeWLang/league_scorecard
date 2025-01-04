'''
TODO:
    * Add in a refresher that grabs any dead cdn links
    * I may need to resize images, but that's no problem
'''
import re
import requests
import json
import pandas as pd
from r2_upload import init_client, upload_new, upload_file

OUTPUT_DIR = 'r2_images'

with open('secrets.json', 'r') as f:
    secrets = json.loads(f.read())
DISCORD_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, REFRESH_TOKEN, *_ = tuple(e for e in secrets.values())
with open('constants.json', 'r') as f:
    const = json.loads(f.read())
COL_NAME_MAP, SHEET_ID, SHEET_NAME, PARSED_LINKS, IMG_FNAME_PATT, R2_DIR = tuple(e for e in const.values())

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
    w = pd.DataFrame(values[1:])
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
    attachments = [att['url'] for att in attachments]
    return attachments


def get_attachments(channel_id, message_id):
    message = retrieve_message(channel_id, message_id)
    attachments = retrieve_attachment_links(message)
    return attachments


def get_all_attachments(ids):
    all_attachments = [get_attachments(id[0], id[1]) for id in ids]
    all_attachments = [y for x in all_attachments for y in x]
    return all_attachments


def gen_ids(data):
    try:
        data.id = data.id.fillna(data.id.isna().cumsum() + data.id.max())
    except AttributeError:
        data['id'] = data.index
    return data


def gen_img_filename(id):
    return f'{IMG_FNAME_PATT}_{int(id)}.png'
    


def gather_img(run, link, img_name):
    img_fname = f'{OUTPUT_DIR}/{img_name}'
    if run:
        r = requests.get(link)
        with open(img_fname, 'wb') as f:
            for chunk in r:
                f.write(chunk)
        print(f'retrieved img: {img_name}')


def main():
    r2_client = init_client()
    new_data = get_sheets_data()
    new_data = clean_sheets_data(new_data)
    new_data, parsed_data = get_unparsed_links(new_data)
    new_data['isnew'] = True
    if len(new_data) > 0:
        new_data['msg_links'] = new_data.form_link.apply(get_discord_ids)
        new_data['img_links'] = new_data.msg_links.apply(get_all_attachments)
        new_data = new_data.explode('img_links')[['time', 'username', 'form_link', 'img_links', 'isnew']].reset_index(drop=True)
        new_data = new_data.rename({'img_links': 'img_link'}, axis=1)
        parsed_data = pd.concat([parsed_data, new_data])
        parsed_data = gen_ids(parsed_data)
        parsed_data['img_filename'] = parsed_data.id.apply(gen_img_filename)
        parsed_data.apply(lambda x: gather_img(x.isnew, x.img_link, x.img_filename), axis=1)
        # Don't reupload since you should only be uploading new
        parsed_data.apply(lambda x: upload_new(r2_client, x.isnew, f'{R2_DIR}/{x.img_filename}', x.img_filename), axis=1)
        parsed_data['isnew'] = False
    parsed_data.to_csv(PARSED_LINKS, index=False)
    upload_file(r2_client, PARSED_LINKS, PARSED_LINKS, True)


if __name__ == '__main__':
    main()
