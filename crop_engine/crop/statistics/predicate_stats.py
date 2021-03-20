import csv
import json
import os
cwd = os.getcwd()


class PredicateStaistics(object):


    def __init__(self, stats_file=None, **kwargs):

        self.__object_auths = {}
        self.__subject_auths = {}
        self.__predicate_counts = {}
        self.__all_subject_auths = set()
        self.__all_object_auths = set()

        if stats_file:
            try:
                with open(stats_file) as tsvfile:
                    tsvreader = csv.reader(tsvfile, delimiter="\t")
                    for line in tsvreader:
                        p = line[0]
                        self.__predicate_counts[p] = int(line[3])
                        subject_auths = set(line[1].split(";"))
                        self.__subject_auths[p] = subject_auths
                        self.__all_subject_auths.update(subject_auths)

                        if ";" in line[2]:
                            object_auths = set(line[2].split(";"))
                            self.__object_auths[p] = object_auths
                            self.__all_object_auths.update(object_auths)
                        elif line[2] != "" and line[2] != " ":
                            self.__object_auths[p] = set([line[2]])

            except Exception as e:
                print("Could not read stats for '{}'".format(stats_file))
                raise e

        elif kwargs.get("stats_list", None):
            for predicate in kwargs['stats_list']:
                self.__predicate_counts[predicate] = 1


    @property
    def predicates(self):
        return set(self.__subject_auths.keys())

    @property
    def predicates_objects(self):
        return set(self.__object_auths.keys())

    def predicate_count(self, predicate):
        return self.__predicate_counts.get(predicate, 0)

    def object_authorities(self, predicate=None):
        if predicate:
            return self.__object_auths[predicate]
        else:
            return self.__all_object_auths

    def subject_authorities(self, predicate=None):
        if predicate:
            return self.__subject_auths[predicate]
        else:
            return self.__all_subject_auths

    def update_subject_authorities(self, predicate, auths):
        self.__subject_auths.setdefault(predicate, set()).update(auths)
        self.__all_subject_auths.update(auths)

    def update_object_authorities(self, predicate, auths):
        self.__object_auths.setdefault(predicate, set()).update(auths)
        self.__all_object_auths.update(auths)

class FederationPredicateStatistic(object):

    def __init__(self, statistcs_fn_map=None, statistics_dict=None):

        self.__sources_stats = {}
        self.__inverse_index = {}
        if statistcs_fn_map:
            self.stats_fn = statistcs_fn_map
            if not isinstance(statistcs_fn_map, dict):
                with open(statistcs_fn_map) as infile:
                    statistcs_fn_map = json.load(infile)

            for source, stats_fn in statistcs_fn_map.items():
                predicate_stats = PredicateStaistics(stats_fn)
                self.__sources_stats[source] = predicate_stats

                for predicate in predicate_stats.predicates:
                    self.__inverse_index.setdefault(predicate, set()).add(source)

        if statistics_dict:

            for source, pred_list in statistics_dict.items():
                predicate_stats = PredicateStaistics(stats_list=pred_list)
                self.__sources_stats[source] = predicate_stats

                for predicate in predicate_stats.predicates:
                    self.__inverse_index.setdefault(predicate, set()).add(source)

    def __str__(self):
        return "FederationPredicateStatistic ({})".format(self.stats_fn)

    def __repr__(self):
        return str(self)

    @property
    def sources(self):
        return self.__sources_stats.keys()


    def all_subject_authorities(self):
        all_auths = set()
        for _, stats in self.__sources_stats.items():
            all_auths.update(stats.subject_authorities)
        return all_auths

    def all_obbject_authorities(self):
        all_auths = set()
        for _, stats in self.__sources_stats.items():
            all_auths.update(stats.obbject_authorities)
        return all_auths


    def sources_by_predicate(self, predicate):
        return self.__inverse_index.get(predicate, set())


    def subject_authorities(self, predicate=None, **kwargs):
        subject_auths = {}
        all_authorities = set()
        sources = kwargs.get("sources", self.__sources_stats.keys())
        for source in sources:
            stats = self.__sources_stats[source]
            if not predicate or predicate in stats.predicates:
                sauths = stats.subject_authorities(predicate)
                all_authorities.update(sauths)
                subject_auths[source] = sauths
        return subject_auths, all_authorities


    def object_authorities(self, predicate, **kwargs):
        object_auths = {}
        all_authorities = set()
        sources = kwargs.get("sources", self.__sources_stats.keys())
        for source in sources:
            stats = self.__sources_stats[source]
            if predicate in stats.predicates_objects:
                oauths = stats.object_authorities(predicate)
                all_authorities.update(oauths)
                object_auths[source] = oauths
        return object_auths, all_authorities


    def predicate_counts(self, predicate, **kwargs):
        predicate_counts = {}
        if str(predicate)[0] == "<" and str(predicate)[-1] == ">":
            predicate = str(predicate)[1:-1]
        sources = kwargs.get("sources", self.__sources_stats.keys())
        for source in sources:
            stats = self.__sources_stats[source]
            cnt = stats.predicate_count(predicate)
            if cnt > 0:
                predicate_counts[source] = cnt
        return predicate_counts

    def update_authorities(self, predicate, source_auths):

        for source, auths in source_auths.items():
            if source in self.__sources_stats.keys():
                self.__sources_stats[source].update_subject_authorities(predicate, auths[0])
                self.__sources_stats[source].update_object_authorities(predicate, auths[0])


if __name__ == '__main__':
    stats_fn = "../stats/predicate_stats.json"
    with open(stats_fn) as infile:
        stats_dct = json.load(infile)

    federation_stats = FederationPredicateStatistic(stats_fn)

    p = "http://bio2rdf.org/ns/chebi#xSource"
    p = "http://www.w3.org/2000/01/rdf-schema#label"

    ep = federation_stats.sources_by_predicate(p)
    print ep

    sauths, all_auths = federation_stats.subject_authorities(p)
    print sauths, all_auths

    oauths, all_auths = federation_stats.object_authorities(p)
    print oauths, all_auths