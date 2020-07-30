import json


def generate_json_for_copy_query(list_data: list) -> str:
    return "\n".join([json.dumps(data) for data in list_data])
