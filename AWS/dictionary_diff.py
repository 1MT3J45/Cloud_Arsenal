NewImage = {
    "channel_name": {
        "S": "Banca Alliances"
    },
    "employee_designation": {
        "S": "CSM"
    },
    "partner_id": {
        "S": "Axis.Ayyappa-Arcade-CP"
    },
    "subchannel_name": {
        "S": "Banca Alliances"
    },
    "template_id": {
        "S": "3"
    },
    "updated_dttm": {
        "S": "2020-03-24T09:27:56.002301Z"
    },
    "user_id": {
        "S": "126867"
    }
}

OldImage = {
    "channel_name": {
        "S": "Banca Alliances"
    },
    "employee_designation": {
        "S": "CSM"
    },
    "partner_id": {
        "S": "Axis.Ayyappa-Arcade-CP"
    },
    "subchannel_name": {
        "S": "Banca Alliances"
    },
    "template_id": {
        "S": "1"
    },
    "updated_dttm": {
        "S": "2020-03-24T09:27:56.002301Z"
    },
    "user_id": {
        "S": "126867"
    }
}

diff = []
for k, v in NewImage.items():
    v1 = OldImage.get(k)
    if v1 is not None:
        if v != v1:
            diff.append(k)


def fast_dict_diff(NewImage, OldImage):
    return [k for k, v in NewImage.items() if OldImage.get(k) is not None if v != OldImage.get(k)]
