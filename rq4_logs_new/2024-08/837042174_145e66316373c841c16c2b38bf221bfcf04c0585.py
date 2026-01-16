from utils.parsers import parse_config, extract_values_to_search


############ parse_config ############
# 1 valid case
# 1 invalid case
def test_parse_config_valid():
    """When passed a config file, it should return true if the config is successfully parsed and the values are successfully read"""

    config = parse_config("tests/templates/config_valid.json")
    assert config["should"] == ["Sample Text"]
    assert config["shouldNot"] == ["Another Sample Text"]


def test_parse_config_invalid():
    """When passed a config file, it should return false if the config is not successfully parsed"""

    config = parse_config("tests/templates/config_invalid_json.json")
    assert config is None


############ parse_config ############
# 1 valid case
# 1 invalid case
def test_extract_values_to_search_valid_config():
    """When passed a valid config, it should return source as config with the values sucessfully extracted from the config"""

    # config with both keys
    data = {"config": "tests/templates/config_valid.json"}
    result = extract_values_to_search(data)
    assert result["source"] == "config"
    assert result["data"]["should"] == ["Sample Text"]
    assert result["data"]["shouldNot"] == ["Another Sample Text"]

    # config with a single key
    data2 = {"config": "tests/templates/config_empty_values.json"}
    result2 = extract_values_to_search(data2)
    assert result2["source"] == "config"
    assert result2["data"]["should"] == ["Sample Text"]
    assert result2["data"]["shouldNot"] == []


def test_extract_values_to_search_valid_commandline():
    """When passed a valid commandline, it should return source as commandline with the values sucessfully extracted from the commandline"""

    # commandline with both keys
    data = {"should": "Sample Text", "should_not": "Another Sample Text"}
    result = extract_values_to_search(data)
    assert result["source"] == "command-line arguments"
    assert result["data"]["should"] == "Sample Text"
    assert result["data"]["shouldNot"] == "Another Sample Text"

    # commandline with a single key
    data2 = {"should": "Sample Text"}
    result2 = extract_values_to_search(data2)
    assert result2["source"] == "command-line arguments"
    assert result2["data"]["should"] == "Sample Text"
    assert result2["data"]["shouldNot"] == ""