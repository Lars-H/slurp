def parse_response(queue, results, **kwargs):

    cnt = 0
    for result in results:
        res = {}
        for var, value in result.items():

            try:
                var_key = str(var)
                val_str = str(value['value'])
            except UnicodeEncodeError:
                var_key = var.encode("utf-8")
                val_str = value['value'].encode("utf-8")

            res[var_key] = val_str

            if value['type'] == "literal":
                if "xml:lang" in value.keys():
                    res[var_key] = '"{}"@{}'.format(val_str, str(value['xml:lang']))
                elif "datatype" in value.keys():
                    res[var_key] = '"{}"^{}'.format(val_str, str(value['datatype:lang']))
                else:
                    res[var_key] = '{}'.format(val_str)
        queue.put(res)
        cnt += 1
    return cnt