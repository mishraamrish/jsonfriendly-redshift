import json
import re

from dateutil import parser


def flatten_dict_for_redshift(json_data):
    out = {}

    # makes name lower case and fixes the max length limit to last 127 char (127 bytes)
    def fix_name(name):
        return name[-127:].lower()

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], fix_name(name + a + '_'))
        elif isinstance(x, list):
            out[fix_name(name[:-1])] = json.dumps(x)
        else:
            try:
                # if datetime object fix it to standard format
                if 9 < len(x) < 40:
                    pdt = parser.parse(x)
                    if not pdt.hour and not pdt.minute and not pdt.second and not pdt.tzinfo:
                        pdt = pdt.date()
                    out[fix_name(name[:-1])] = str(pdt)
                else:
                    out[fix_name(name[:-1])] = x
            except:
                out[fix_name(name[:-1])] = x
    flatten(json_data)
    return out


def flatten_and_fix_timestamp(json_data, **mappings):
    out = {}

    def key_mapping(key, mapper):
        return_name = mapper.get(key)
        if return_name:
            return return_name
        return key

    def flatten(x, name=''):
        key_name = key_mapping(name[:-1], mappings)
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], key_mapping(name + a + '_', mappings))
        elif isinstance(x, list):
            out[key_name] = json.dumps(x)
        else:
            try:
                # if datetime object fix it to standard format
                if 9 < len(x) < 40:
                    pdt = parser.parse(x)
                    if not pdt.hour and not pdt.minute and not pdt.second and not pdt.tzinfo:
                        pdt = pdt.date()
                    out[key_name] = str(pdt)
                else:
                    out[key_name] = x
            except:
                out[key_name] = x
    flatten(json_data)
    return out


def fix_keys(v):
    def convert_list(lst):
        return [fix_keys(item) for item in lst]

    def convert_dict(d):
        return {re.sub(r"[^a-zA-Z0-9_]+", "", str(key)): fix_keys(value) for key, value in d.items()}
    if isinstance(v, dict):
        return convert_dict(v)
    elif isinstance(v, list):
        return convert_list(v)
    else:
        return v
