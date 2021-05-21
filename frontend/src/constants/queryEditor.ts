export const DEFAULT_QUERY = `SELECT * WHERE {
	?d1 <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Alcohols> .
	?d2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/class/yago/Alcohols> .
	?d1 <http://dbpedia.org/property/routesOfAdministration> ?o .
	?d2 <http://dbpedia.org/property/routesOfAdministration> ?o .
}`;
