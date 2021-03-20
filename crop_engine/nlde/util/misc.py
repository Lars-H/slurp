import datetime

data_types = {
    'integer': (int, 'numerical'),
    'decimal': (float, 'numerical'),
    'float': (float, 'numerical'),
    'double': (float, 'numerical'),
    'string': (str, str),
    'boolean': (bool, bool),
    'dateTime': (datetime, datetime),
    'nonPositiveInteger': (int, 'numerical'),
    'negativeInteger': (int, 'numerical'),
    'long': (long, 'numerical'),
    'int': (int, 'numerical'),
    'short': (int, 'numerical'),
    'byte': (bytes, bytes),
    'nonNegativeInteger': (int, 'numerical'),
    'unsignedLong': (long, 'numerical'),
    'unsignedInt': (int, 'numerical'),
    'unsignedShort': (int, 'numerical'),
    'unsignedByte': (bytes, bytes),  # TODO: this is not correct
    'positiveInteger': (int, 'numerical')
}

def median(lst):
    n = len(lst)
    s = sorted(lst)
    return (sum(s[n//2-1:n//2+1])/2.0, s[n//2])[n % 2] if n else None


def extractValue(val):
    dt = None
    if isinstance(val, str) and "^^" in val:
        val, dt = val.split("^^")
        val = val[1:-1]
    # Handles when the literal is typed.
    if dt:
        for t in data_types.keys():
            if (t in dt):
                (python_type, general_type) = data_types[t]

                if (general_type == bool):
                    return (val, general_type)

                else:
                    return (python_type(val), general_type)
    else:
        #try:
        #    val = float(val.replace('"', "").replace("'", ""))
        #    return (val, 'numerical')
        #except:
        #    pass
        if val[0] == '"' and val[-1] == '"':
            val = val[1:-1]
        return (val, str)


def compatible_solutions(mu1, mu2):
    common_vars = set(mu1.keys()).intersection(set(mu2.keys()))
    for common_var in common_vars:
        if mu1[common_var] != mu2[common_var]:
            return False
    return True