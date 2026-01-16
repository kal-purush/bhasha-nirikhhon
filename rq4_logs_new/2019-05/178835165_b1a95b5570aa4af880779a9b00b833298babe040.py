def process_script(script):
    results = {}
    for table in script.table:
        if table["key"] == "validity":
            results["validity"] = {}
            for elem in table.elem:
                results["validity"][elem["key"]] = elem.cdata
        if table["key"] == "issuer" or table["key"] == "subject":
            results[table["key"]] = {}
            for elem in table.elem:
                results[table["key"]][elem["key"]] = elem.cdata
        if table["key"] == "extensions":
            results["extensions"] = []
            for ext_table in table.table:
                alt_name = {}
                alt_rec = False
                for elem in ext_table.elem:
                    if elem["key"] == "name" and "Subject Alternative Name" in elem.cdata:
                        alt_rec = True
                    alt_name[elem["key"]] = elem.cdata
                if alt_rec:
                    results['extensions'].append(alt_name)
    return {'ssl_cert': results}