def describe_city(name, country='Chad'):
    """Print some info about this city"""
    print(f"{name.title()} is in the country of {country.title()}")

describe_city('NYC')
describe_city(name='London',country='Englnd')