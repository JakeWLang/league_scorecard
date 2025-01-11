import math
import json
from PIL import Image

with open('constants.json', 'r') as f:
    constants = json.loads(f.read())
*_, TOO_TALL_THRESH, TALL_DOWNSIZE = tuple(e for e in constants.values())


def resize_tall(filepath):
    image = Image.open(filepath)
    width, height = image.size[0], image.size[1]
    img_ratio = width / height
    if img_ratio < float(TOO_TALL_THRESH):
        print(f'resizing {filepath} because it is too damn tall')
        goal_height = math.floor(image.size[1] * float(TALL_DOWNSIZE))
        goal_width = math.floor(goal_height * img_ratio)
        new_image = image.resize((goal_width, goal_height))
        new_image.save(filepath)
