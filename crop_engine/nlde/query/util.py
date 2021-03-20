def aux(e,x, op):
    def pp (t):
        return t.show(x+"  ")
    if type(e) == tuple:
        (f,s) = e
        r = ""
        if f:
            r = x+"{\n"+ aux(f, x+"  ", op) + "\n" + x + "}\n"
        if f and s:
            r = r + x + op + "\n"
        if s:
            r = r + x+"{\n" + aux(s,x+"  ", op) +"\n"+x+"}"
        return r
    elif type(e) == list:
        return (x + " . \n").join(map(pp, e))
    elif e:
        return e.show(x+"  ")
    return ""

def nest(l):

    l0 = list(l)
    while len(l0) > 1:
        l1 = []
        while len(l0) > 1:
            x = l0.pop()
            y = l0.pop()
            l1.append((x,y))
        if len(l0) == 1:
            l1.append(l0.pop())
        l0 = l1
    if len(l0) == 1:
        return l0[0]
    else:
        return None