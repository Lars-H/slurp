from nlde.query import TriplePattern

class NaiveSourceSelection(object):

    @staticmethod
    def select_sources(triple_patterns, source_selector, **kwargs):

        verbose = kwargs.get("verbose", True)
        threshold = kwargs.get("threshold", -1)
        tp_groups = []
        for triple_pattern in triple_patterns:
            sources_per_tp = []
            if not triple_pattern.count:
                tp_cnt = source_selector.select_sources(triple_pattern)
            else:
                tp_cnt = triple_pattern.count
            if tp_cnt > 0:
                if not verbose:
                    sources_per_tp.append(triple_pattern)
                elif threshold > 0 and len(triple_pattern.sources.keys()) > threshold:
                    sources_per_tp.append(triple_pattern)
                else:
                    for source, cnt in triple_pattern.sources.items():
                        tp = TriplePattern(triple_pattern[0], triple_pattern[1], triple_pattern[2])
                        tp.sources[source] = cnt
                        tp.cardinality = cnt
                        sources_per_tp.append(tp)
            tp_groups.append(sources_per_tp)
        return tp_groups

