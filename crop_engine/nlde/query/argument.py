

class Argument(object):
    def __init__(self, value, isconstant=False, desc=False):
        self.value = value
        self.isconstant = self.isuri() or self.isliteral()
        self.desc = desc

    def __repr__(self):
        return str(self.value)

    def isuri(self):
        if self.value[0] == "<":
            return True
        else:
            return False

    def isvariable(self):
        if (self.value[0] == "?") or (self.value[0] == "$"):
            return True
        else:
            return False

    def isbnode(self):
        if self.value[0] == "_":
            return True
        else:
            return False

    def isliteral(self):
        if self.value[0] == '"' or self.value[0].isdigit():
            return True
        else:
            return False

    def isfloat(self):
        if self.value[0].isdigit():
            if "." in self.value:
                try:
                    float(self.value)
                    return True
                except:
                    return False
        return False

    def isint(self):
        if self.value[0].isdigit():
            if not "." in self.value:
                try:
                    int(self.value)
                    return True
                except:
                    return False
        return False

    def get_variable(self):
        if self.isvariable():
            return self.value[1:]

    def to_numerical(self):
        val = self.value
        if "^^" in val:
            val = val.split("^^")[0]

        try:
            return float(val)
        except:
            pass

        try:
            return int(val)
        except:
            pass
        finally:
            return None

    @property
    def name(self):
        return self.value