def process_script(script):
    script_info = []
    for proto_table in script.table:
        proto_info = {
            "protocol": proto_table["key"]
        }
        for sub_table in proto_table.table:
            if sub_table["key"] == "ciphers":
                proto_info["ciphers"] = cipher_info = []
                process_ciphers(cipher_info, sub_table)
        for elem in proto_table.elem:
            if elem["key"] == "cipher preference":
                proto_info["cipher_preference"] = elem.cdata
        script_info.append(proto_info)

    result = {"ssl_enum_ciphers": script_info}
    for elem in script.elem:
        if elem["key"] == "least strength":
            result["ssl_least_strength"] = elem.cdata
    return result


def process_ciphers(cipher_info, table):
    for cipher_table in table.table:
        info = {}
        for elem in cipher_table.elem:
            info[elem["key"]] = elem.cdata
        cipher_info.append(info)