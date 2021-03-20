from urlparse import urlparse



def get_authorities(server, answers, context, triple_pattern):

    var_positions = triple_pattern.variable_position

    # ?s <p> ?o
    if var_positions == 5:
        return parse_subject_object_auths(server, answers, context)

    # ?s <p> <o>
    elif var_positions == 4:
        s_auths, o_auths =  parse_subject_object_auths(server, answers, context)
        if triple_pattern[2].isuri() or str(triple_pattern[2]).startswith("http"):
            object_uri = triple_pattern[2].value
            authority = urlparse(object_uri).netloc
            o_auths.add(authority)
        return s_auths, o_auths

    # <s> ?p ?o
    elif var_positions == 3:
        s_auths, o_auths =  parse_object_predicate_authorities(server, answers, context)
        subject_uri = triple_pattern[0].value
        authority = urlparse(subject_uri).netloc
        s_auths.add(authority)
        return s_auths, o_auths

    # ?s ?p <o>
    elif var_positions == 6:
        s_auths, o_auths =  parse_subject_object_auths(server, answers, context)
        subject_uri = triple_pattern[0].value
        authority = urlparse(subject_uri).netloc
        s_auths.add(authority)
        return s_auths, o_auths

    # <s> <p> ?o
    elif var_positions == 1:
        s_auths, o_auths =  parse_object_authorities(server, answers, context)
        subject_uri = triple_pattern[0].value
        authority = urlparse(subject_uri).netloc
        s_auths.add(authority)
        return s_auths, o_auths

    # <s> ?p <o>
    elif var_positions == 2:
        s_auths = set()
        o_auths = set()
        subject_uri = triple_pattern[0].value
        authority = urlparse(subject_uri).netloc
        s_auths.add(authority)
        if triple_pattern[2].isuri() or str(triple_pattern[2]).startswith("http"):
            object_uri = triple_pattern[2].value
            authority = urlparse(object_uri).netloc
            o_auths.add(authority)
        return s_auths, o_auths

    else:
        return set(), set()


def parse_subject_object_auths(server, answers, context):

    s_auths = set()
    o_auths = set()
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
            s = elem["@id"]
            pos = s.find(":")

            if pos != -1:
                prefix = s[0:pos]
                if prefix in context.keys():
                    s = s.replace(prefix + ":", context[prefix])
            else:
                prefix = s
                if "http" not in prefix:
                    s = context[prefix]["@id"]
            authortiy = urlparse(s).netloc
            s_auths.add(authortiy)
            del elem["@id"]

            p = elem.keys()[0]

            if isinstance(elem[p], dict):
                if "@id" in elem[p].keys():
                    o = elem[p]["@id"]
                    pos = o.find(":")

                    if pos != -1:
                        prefix = o[0:pos]
                        if prefix in context.keys():
                            o = o.replace(prefix + ":", context[prefix])
                    else:
                        prefix = o
                        if "http" not in prefix:
                            o = context[prefix]["@id"]

                    authortiy = urlparse(o).netloc
                    o_auths.add(authortiy)

    return s_auths, o_auths

def parse_object_authorities(server, answers, context):
    o_auths = set()
    to_process = {}
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
        #if "@graph" not in elem.keys() and "hydra" not in str(elem) and "void" not in str(elem) and "variable" not in str(elem) and "template" not in str(elem):
            to_process = elem

    to_process.pop("@id", None)
    p = to_process.keys()[0]
    answers = to_process

    if isinstance(answers[p], dict):
        elem = answers[p]
        if "@id" in elem.keys():
            o = elem["@id"]
            pos = o.find(":")
            prefix = o[0:pos]
            if prefix in context.keys():
                o = o.replace(prefix + ":", context[prefix])
            authortiy = urlparse(o).netloc
            o_auths.add(authortiy)

    elif not(isinstance(answers[p], list)):
        elem = answers[p]
        if not isinstance(elem, float) and isinstance(elem, int):
            if elem[0] != "<" and elem[-1] != ">" and ":" in elem:
                prefix, suffix = elem.split(":")
                if prefix in context.keys():
                    elem = "{}{}".format(context[prefix],suffix)
            authortiy = urlparse(elem).netloc
            o_auths.add(authortiy)

    else:

        for elem in answers[p]:
            if isinstance(elem, dict):
                if "@id" in elem.keys():
                    o = elem["@id"]
                    pos = o.find(":")
                    if pos != -1:
                        prefix = o[0:pos]
                        if prefix in context.keys():
                            o = o.replace(prefix + ":", context[prefix])
                    else:
                        if "http" not in o:
                            prefix = o
                            o = context[prefix]["@id"]
                    authortiy = urlparse(o).netloc
                    o_auths.add(authortiy)
            else:
                if not isinstance(elem, float) and isinstance(elem, int):
                    if elem[0] != "<" and elem[-1] != ">" and ":" in elem:
                        prefix, suffix = elem.split(":")
                        if prefix in context.keys():
                            elem = "{}{}".format(context[prefix], suffix)
                    authortiy = urlparse(elem).netloc
                    o_auths.add(authortiy)
    return set(), o_auths

def parse_object_predicate_authorities(server, answers, context):


    o_auths = set()
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():

            for p in elem.keys():
                if isinstance(elem[p], dict):

                    if "@id" in elem[p].keys():
                        o = elem[p]["@id"]
                        pos = o.find(":")

                        if pos != -1:
                            prefix = o[0:pos]
                            if prefix in context.keys():
                                o = o.replace(prefix + ":", context[prefix])
                        else:
                            prefix = o
                            if "http" not in prefix:
                                o = context[prefix]["@id"]
                        authortiy = urlparse(o).netloc
                        o_auths.add(authortiy)


                elif isinstance(elem[p], list):
                    for oelem in elem[p]:
                        if isinstance(oelem, dict):
                            if "@id" in oelem.keys():
                                o = oelem["@id"]
                                pos = o.find(":")
                                prefix = o[0:pos]
                                if prefix in context.keys ():
                                    o = o.replace(prefix + ":", context[prefix])
                                authortiy = urlparse(o).netloc
                                o_auths.add(authortiy)
    return set(), o_auths
