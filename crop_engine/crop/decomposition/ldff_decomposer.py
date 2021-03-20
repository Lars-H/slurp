from itertools import combinations
from nlde.query import BGP, Filter
from nlde.engine.contact_source import get_metadata

class LDFF_Decomposer(object):


    @staticmethod
    def get_decomposition(triple_patterns):
        Q = triple_patterns
        change = True
        while change:

            for sq_i, sq_j in combinations(Q,2):
                if isinstance(sq_i, Filter) or isinstance(sq_j, Filter):
                    continue
                if sq_i.compatible(sq_j):
                    if len(sq_i.sources.keys()) == 1 and len(sq_j.sources.keys()) == 1 \
                        and len(set(sq_i.sources.keys()).intersection(set(sq_j.sources.keys()))) == 1:
                            if sq_i.sources.keys()[0].startswith("sparql@"):
                                Q.remove(sq_j)
                                Q.remove(sq_i)

                                if isinstance(sq_i, BGP):
                                    tps = sq_i.triple_patterns
                                else:
                                    tps = [sq_i]

                                if isinstance(sq_j, BGP):
                                    tps.extend(sq_j.triple_patterns)
                                else:
                                    tps.append(sq_j)

                                new_sq = BGP(tps)
                                Q.append(new_sq)
                                break
            else:
                change = False

        # Update Cardinalities if a BGP was created
        for sq in Q:
            if isinstance(sq, BGP):
                sq.cardinality = get_metadata(sq.sources.keys(), sq)

        return Q