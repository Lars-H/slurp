from .naive import NaiveSourceSelection
from itertools import combinations
from urlparse import urlparse

class StarBasedSourceSelection(object):

    @staticmethod
    def select_sources(triple_patterns, source_selector, **kwargs):

        mode = kwargs.get("mode", "star")

        if mode == "star":
            return StarBasedSourceSelection.common_predicate_selection(triple_patterns, source_selector, **kwargs)
        elif mode == "auth":
            return StarBasedSourceSelection.intrastar_optimized_selection(triple_patterns, source_selector, **kwargs)


    @staticmethod
    def common_predicate_selection(triple_patterns, source_selector, **kwargs):
        sources = source_selector.sources
        rel_sources = set()
        for triple_pattern in triple_patterns:
            if not triple_pattern.count:
                tp_cnt = source_selector.select_sources(triple_pattern)
            tp_sources = set(triple_pattern.sources.keys())
            if len(rel_sources) == 0:
                rel_sources= set(tp_sources)
            else:
                rel_sources = rel_sources.intersection(tp_sources)

        del_keys = set(sources) - rel_sources

        for triple_pattern in triple_patterns:
            for del_key in del_keys:
                if del_key in triple_pattern.sources.keys():
                    del triple_pattern.sources[del_key]

        return NaiveSourceSelection.select_sources(triple_patterns, source_selector, **kwargs)


    @staticmethod
    def intrastar_optimized_selection(triple_patterns, source_selector, **kwargs):

        predicate_stats = kwargs.get("predicate_stats", None)
        if not predicate_stats: raise ValueError

        tps2auths = {}
        tps2source2auths = {}
        for triple_pattern in triple_patterns:
            if not triple_pattern.count:
                tp_cnt = source_selector.select_sources(triple_pattern)

            tp_type = triple_pattern.variable_position


            # Case ?s <p> ?o or ?s <p> <o>
            if tp_type > 3:
                if tp_type > 5:
                    predicate = None
                else:
                    predicate = triple_pattern[1].value[1:-1]
                source2auths, all_auths = predicate_stats.subject_authorities(predicate)
                tps2auths[triple_pattern] = all_auths
                tps2source2auths[triple_pattern] = source2auths
            elif tp_type < 4:
                s_auth = urlparse(triple_pattern[0].value[1:-1]).netloc
                s_auth_set = set([s_auth])
                tps2auths[triple_pattern] = s_auth_set
                tps2source2auths[triple_pattern] = { source : s_auth_set for source in triple_pattern.sources.keys() }

        common_sauths = None
        for tp, auths in tps2auths.items():
            if not common_sauths:
                common_sauths = auths
            else:
                common_sauths = common_sauths.intersection(auths)

        for triple_pattern in triple_patterns:
            for source in triple_pattern.sources.keys():
                if len(tps2source2auths[triple_pattern][source].intersection(common_sauths)) == 0:
                    # Reduce Excpected Results and remove source
                    triple_pattern.cardinality = triple_pattern.cardinality - triple_pattern.sources[source]
                    del triple_pattern.sources[source]

            triple_pattern.subject_auths = tps2source2auths[triple_pattern]
            predicate = triple_pattern[1].value[1:-1]
            object_auths = predicate_stats.object_authorities(predicate, sources=triple_pattern.sources.keys())[0]
            triple_pattern.object_auths = object_auths


        return NaiveSourceSelection.select_sources(triple_patterns, source_selector, **kwargs)

    @staticmethod
    def interstar_optimized_selection(ssq):

        for ssq1, ssq2 in combinations(ssq, 2):
            join_vars = ssq1.vars.intersection(ssq2.vars)
            if len(join_vars) > 0:
                for join_var in join_vars:
                    pos1 = ssq1.get_variable_position(join_var)
                    pos2 = ssq2.get_variable_position(join_var)
                    ssq1_auths = ssq1.variable_authorities(pos1)
                    ssq2_auths = ssq2.variable_authorities(pos2)
                    common_var_authorities = ssq1_auths.intersection(ssq2_auths)
                    ssq1_deleted_sources = ssq1.delete_sources(common_var_authorities, pos1, join_var)
                    ssq2_deleted_sources = ssq2.delete_sources(common_var_authorities, pos2, join_var)

                    ssq1.remove_from_source_group(ssq1_deleted_sources)
                    ssq2.remove_from_source_group(ssq2_deleted_sources)
                    i = 1
