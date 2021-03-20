from nlde.engine.contact_source import get_metadata

class StarSubquery(object):

    def __init__(self, triple_patterns, var):

        self.triple_patterns = triple_patterns
        self.join_var = var
        self.source_groups = None

    def __str__(self):
        return " ".join([str(tp) for tp in self.triple_patterns])

    def __repr__(self):
        return str(self)

    @property
    def vars(self):
        vars = set()
        for tp in self.triple_patterns:
            vars.update(tp.variables)
        return vars


    def get_variable_position(self, var):
        for tp in self.triple_patterns:
            pos = tp.get_variable_position(var)
            if pos >= 1:
                var_position = pos
                return var_position
        else:
            return 0


    def variable_authorities(self, position):
        auths = set()
        for tp in self.triple_patterns:
            if position == 4:
                sauths = set()
                for source_sets in tp.subject_auths.values():
                    sauths.update(source_sets)
                auths.update(sauths)
            elif position == 1:
                oauths = set()
                for source_sets in tp.object_auths.values():
                    oauths.update(source_sets)
                auths.update(oauths)
        return auths

    def delete_sources(self, common_authorities, postion, join_var):
        deleted_source = []

        for tp in self.triple_patterns:
            if postion == 4 and tp[0].isvariable() and tp[0].get_variable() == join_var:
                for source, auths in tp.subject_auths.items():
                    if len(auths.intersection(common_authorities)) == 0:
                        if source in tp.sources.keys():
                            tp.cardinality = tp.cardinality - tp.sources[source]
                            del tp.sources[source]
                            deleted_source.append(source)
            elif postion == 1 and tp[2].isvariable() and tp[2].get_variable() == join_var:
                for source, auths in tp.object_auths.items():
                    if len(auths.intersection(common_authorities)) == 0:
                        if source in tp.sources.keys():
                            tp.cardinality = tp.cardinality - tp.sources[source]
                            del tp.sources[source]
                            deleted_source.append(source)

        return deleted_source

    def remove_from_source_group(self, sources):
        new_source_groups = []
        for sg in self.source_groups:
            new_source_group = []
            for tp in sg:
                if len(set(tp.sources.keys()).intersection(sources)) == 0:
                    new_source_group.append(tp)
            new_source_groups.append(new_source_group)

        self.source_groups = new_source_groups

class TriplePatternSourceSelector(object):

    def __init__(self, **kwargs):
        pass

    @property
    def sources(self):
        pass

    def select_sources(self, triple_pattern):
        pass



class AskSourceSelector(TriplePatternSourceSelector):

    def __init__(self, **kwargs):
        self.__sources = kwargs.get("sources")
        self.__stats = kwargs.get("predicate_stats", None)


    def __str__(self):
        return "AskSourceSelector"

    def __repr__(self):
        return str(self)

    @property
    def sources(self):
        return self.__sources

    def select_sources(self, triple_pattern):
        if self.__stats and triple_pattern[1].isuri():
            predicate = triple_pattern[1].value.replace("<", "").replace(">", "")
            # Update stats
            #card, auth_stats = get_metadata_tpf_stats(self.__sources, triple_pattern)
            #self.__stats.update_authorities(predicate,  auth_stats)
            #return card
            return get_metadata(self.__sources, triple_pattern)
        else:
            return get_metadata(self.__sources, triple_pattern)

class StatSourceSelector(TriplePatternSourceSelector):

    def __init__(self, **kwargs):
        self.__sources = kwargs.get("sources")
        self.__stats = kwargs.get("predicate_stats", None)
        self.__full_stats = kwargs.get("full_predicate_stats", None)
        self.processed_predicates = 0
        self.missed_predicate_cnt = 0
        self.missed_predicates = {}
        self.missed_sources = {}

    def __str__(self):
        return "StatSourceSelector"

    def __repr__(self):
        return str(self)

    @property
    def sources(self):
        return self.__sources

    def reset(self):
        self.processed_predicates = 0
        self.missed_predicate_cnt = 0
        self.missed_predicates = {}
        self.missed_sources = {}


    def select_sources(self, triple_pattern):

        if not self.__full_stats:
            get_metadata(self.__sources, triple_pattern)
            true_sources = set([name.replace("tpf@", "") for name in triple_pattern.sources.keys()])
        else:
            true_sources = set(self.__full_stats.predicate_counts(triple_pattern[1]).keys())

        self.processed_predicates+= 1
        sources = self.__stats.predicate_counts(triple_pattern[1])

        delta = true_sources - set(sources).intersection(true_sources)
        #print(len(delta))
        if len(sources) == 0:
            if triple_pattern[1].isuri():
                self.missed_predicate_cnt += 1
                self.missed_predicates[triple_pattern[1]] = list(delta)

        elif len(delta) > 0 and triple_pattern[1].isuri():
            self.missed_sources[triple_pattern[1]] = delta

        triple_pattern.sources = sources


class HybridSourceSelector(object):

    def __init__(self, **kwargs):
        self.__stats = kwargs.get("predicate_stats")

    def __str__(self):
        return "HybridSourceSelector"

    def __repr__(self):
        return str(self)

    @property
    def sources(self):
        return self.__stats.sources

    def select_sources(self, triple_pattern):

        predicate = triple_pattern[1].value[1:-1]
        if triple_pattern.variable_position == 5:
            sources = self.__stats.predicate_counts(predicate)
            triple_pattern.sources = sources
            triple_pattern.cardinality = sum(sources.values())
            return triple_pattern.cardinality
        else:
            relevant_sources = self.__stats.sources_by_predicate(predicate)
            sources = relevant_sources if len(relevant_sources) > 0 else self.sources
            return get_metadata(sources, triple_pattern)



class CSBasedSourceSelector(object):

    def __init__(self, **kwargs):
        self.__stats = kwargs.get("predicate_stats")

    @property
    def sources(self):
        self.__stats.sources

    def select_sources(self, triple_pattern):
        triple_pattern.sources = self.__stats.predicate_counts(triple_pattern[1])
