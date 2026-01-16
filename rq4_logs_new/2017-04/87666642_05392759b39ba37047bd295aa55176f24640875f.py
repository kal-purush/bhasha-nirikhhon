def get_id_from_resource(resource_str):
    return resource_str.split('/')[-1]


def get_id_from_url(url):
    return url.split('/')[-1]