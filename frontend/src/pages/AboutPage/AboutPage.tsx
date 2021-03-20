import React, { Component } from "react";
import {
	Heading,
	Text,
	Code,
	Link,
	Center,
	VStack,
	UnorderedList,
	ListItem,
	AspectRatio,
} from "@chakra-ui/react";

class AboutPage extends Component {
	render() {
		return (
			<>
				<VStack align="left" spacing="24px">
					<Heading size="2xl">SLURP: An Interactive SPARQL Query Planner</Heading>
					<Text mt="3" mb="3">
						Accompanying a submission to Posters &#38; Demos at ESWC 2021
					</Text>
					<Heading size="xl">tl;dr</Heading>
					<Text mt="3" mb="3">
						Here, you find the supplemental material to our Demo Paper &quot;SLURP: An
						Interactive SPARQL Query Planner&quot; including a demonstration video.
						Check out the live demo of SLURP{" "}
						<u>
							<a href="https://km.aifb.kit.edu/sites/slurp/">here</a>
						</u>
						!
					</Text>
					<Heading size="xl">Abstract</Heading>
					<Text mt="3" mb="3">
						Triple Pattern Fragments (TPFs) allow for querying large RDF graphs with
						high availability by offering triple pattern-based access to the graphs.
						This limited expressivity of TPFs leads to higher client-side querying cost
						and the challenge of devising efficient query plans when evaluating SPARQL
						queries. Different heuristics and cost-model based query planning approaches
						have been proposed to obtain such efficient query plans. However, we require
						means to visualize, modify, and execute alternative query plans, to better
						understand the differences between existing planning approaches and their
						potential shortcomings. To this end, we propose SLURP, an interactive SPARQL
						query planner that assists RDF data consumers to visualize, modify, and
						compare the performance of different query execution plans over TPFs.
					</Text>
					<Heading size="xl">The Demo: SLURP</Heading>
					<Text mt="3" mb="3">
						Follow this link to check out{" "}
						<u>
							<a href="https://km.aifb.kit.edu/sites/slurp/">SLURP</a>
						</u>
						.
					</Text>
					<Heading size="xl">Use-Case</Heading>
					<Heading size="md">Motivating example</Heading>
					<Text mt="3" mb="3">
						We consider the following example SPARQL query to demonstrate our tool:
					</Text>
					<Center>
						<figure>
							<Center>
								<Code>
									1 SELECT * WHERE &#123;
									<br />
									2 ?d1 dcterms:subject dbpedia:Category:Alcohols . # Count : 695
									<br />
									3 ?d2 rdf:type yago:Alcohols . # Count : 529
									<br />
									4 ?d1 dbprop:routesOfAdministration ?o . # Count : 2430
									<br />
									5 ?d2 dbprop:routesOfAdministration ?o . # Count : 2430
									<br />6 &#125;
								</Code>
							</Center>
							<figcaption>
								<Text mt="3" mb="3">
									Listing 1: SPARQL query to retrieve resources classified as
									alcohols [1]. Prefixes are used as in http://prefix.cc/
								</Text>
							</figcaption>
						</figure>
					</Center>
					<Text mt="3" mb="3">
						Different query plans can be devised for this SPARQL query. For example, the
						implemented query processing engine by Verborgh et al. [4] produces a
						left-linear plan with Nested Loop Joins which can be seen in Figure 1 on the
						left side. The leaves of these plans represent triple patterns and are
						identified by their line number from Listing 1. The number of matching
						triples for each triple pattern and number of intermediate results are
						indicated in parentheses.
					</Text>
					<Text mt="3" mb="3">
						Other query processing engines [1, 2] use different optimization algorithms
						that are capable to devise more efficient query plans. The implementation of
						Acosta and Vidal [1] executes the query from Listing 1 with a bushy tree
						plan, as depicted in Figure 1 on the right side.
					</Text>
					<Center>
						<figure>
							<img src="./png/plans-motivation.png" width="600"></img>
							<Center>
								<figcaption>
									<Text mt="3" mb="3">
										Figure 1: Left-linear plan (left), Bushy tree plan (right)
										[1, p. 113]
									</Text>
								</figcaption>
							</Center>
						</figure>
					</Center>
					<Text mt="3" mb="3">
						The results for executing the example query of Listing 1 are shown in Table
						1. The execution of the left-linear plan shows that many requests are
						necessary and the overall execution time is rather long. The bushy tree plan
						enables the simultaneous execution of subtrees and makes use of another join
						type, which reduces the execution time and number of requests as shown in
						Table 1 [1, p. 114].
					</Text>
					<Center>
						<figure>
							<table width="400px">
								<thead>
									<tr style={{ textAlign: "left" }}>
										<th>Metrics</th>
										<th>Left-linear</th>
										<th>Bushy tree</th>
									</tr>
								</thead>
								<tbody>
									<tr>
										<td>Execution time (sec.)</td>
										<td>318.90</td>
										<td>3.03</td>
									</tr>
									<tr>
										<td>Results</td>
										<td>1,398</td>
										<td>5,651</td>
									</tr>
									<tr>
										<td>Requests</td>
										<td>1,693</td>
										<td>67</td>
									</tr>
								</tbody>
							</table>
							<Center mt="3">
								<figcaption>Table 1: Execution results [1, p. 113]</figcaption>
							</Center>
						</figure>
					</Center>
					<Text mt="3" mb="3">
						The following figures illustrate the execution of the above mentioned query
						plans with SLURP. Note: We have specified a time out after 60 seconds for
						this demonstration.
						<Center>
							<figure>
								<img src="./png/slurp_left.png" width="600"></img>
								<Center>
									<figcaption>
										<Text mt="3" mb="3">
											Figure 2: Left-linear plan in SLURP
										</Text>
									</figcaption>
								</Center>
							</figure>
						</Center>
						<Center>
							<figure>
								<img src="./png/slurp_bushy.png" width="600"></img>
								<Center>
									<figcaption>
										<Text mt="3" mb="3">
											Figure 3: Bushy tree plan in SLURP
										</Text>
									</figcaption>
								</Center>
							</figure>
						</Center>
					</Text>
					<Text mt="3" mb="3">
						Our tool enables the visual analysis and modification of query plans
						generated by client-side query processing engines. Furthermore, SLURP
						enables the execution of (modified) query plans, of which the results and
						statistics can be reviewed afterwards. Our tool allows researchers to
						efficiently study the impact of different query plans on the performance of
						the query execution using a specific engine.
					</Text>
					<Text>Check out the showcase video of SLURP below!</Text>
					<Heading size="md">Architecture of SLURP</Heading>
					<Text mt="3" mb="3">
						<Center>
							<figure>
								<img src="./png/architecture.png" width="600"></img>
								<Center>
									<figcaption>
										<Text mt="3" mb="3">
											Figure 4: Top-Level architecture of SLURP
										</Text>
									</figcaption>
								</Center>
							</figure>
						</Center>
					</Text>
					<Heading size="md">Evaluation</Heading>
					<Text mt="3" mb="3">
						We have evaluated SLURP in a small user study where the participants had to
						perform two tasks and filled out the System Usabiity Scale (SUS) [5]. SUS is
						an effective and inexpensive tool for assessing the usability of interactive
						systems.
						<br></br>
						The overall <b>SUS score</b> from the study is <b>83.5</b>. This means the
						usability of SLURP is well above average. However, only five participants
						have evaluated SLURP in total. If you want to evaluate SLURP, you can also
						participate in the study:&nbsp;
						<u>
							<Link
								href="https://docs.google.com/forms/d/e/1FAIpQLSd03SQxv3ZYxg-sqBfK-QShik-B4Asi0eG1tJSAoQK0uQj10A/viewform?usp=sf_link"
								target="_blank"
							>
								SLURP User Study
							</Link>
						</u>
					</Text>
					<Heading id="video" size="xl">
						Video Presenting SLURP
					</Heading>
					<AspectRatio ratio={16 / 9}>
						<video width="320" height="240" controls title="SLURP Demo">
							<source
								src="https://people.aifb.kit.edu/zg2916/slurp/media/slurp_demo.mp4"
								type="video/mp4"
							/>
						</video>
					</AspectRatio>
					<Heading size="xl">Code</Heading>
					<Text mt="3" mb="3">
						The repository of our project can be found{" "}
						<u>
							<Link
								href="https://git.scc.kit.edu/zg2916/interactive-planner"
								target="_blank"
							>
								here
							</Link>
						</u>
						.
					</Text>

					<Heading size="xl">Authors</Heading>
					<UnorderedList pl={6}>
						<ListItem>
							<u>
								<Link href="https://github.com/jannikdresselhaus" target="_blank">
									Jannik Dresselhaus
								</Link>
							</u>
							,{" "}
							<u>
								<Link href="http://www.aifb.kit.edu/">
									Institute AIFB, Karlsruhe Institute of Technology (KIT), Germany
								</Link>
							</u>
						</ListItem>
						<ListItem>
							<u>
								<Link
									href="https://www.linkedin.com/in/ilya-filippov/"
									target="_blank"
								>
									Ilya Filippov
								</Link>
							</u>
							,{" "}
							<u>
								<Link href="http://www.aifb.kit.edu/">
									Institute AIFB, Karlsruhe Institute of Technology (KIT), Germany
								</Link>
							</u>
						</ListItem>
						<ListItem>
							<u>
								<Link href="https://github.com/Jogi95" target="_blank">
									Johannes Gengenbach
								</Link>
							</u>
							,{" "}
							<u>
								<Link href="http://www.aifb.kit.edu/">
									Institute AIFB, Karlsruhe Institute of Technology (KIT), Germany
								</Link>
							</u>
						</ListItem>
						<ListItem>
							<u>
								<Link
									href="https://www.aifb.kit.edu/web/Lars_Heling"
									target="_blank"
								>
									Lars Heling
								</Link>
							</u>
							,{" "}
							<u>
								<Link href="http://www.aifb.kit.edu/">
									Institute AIFB, Karlsruhe Institute of Technology (KIT), Germany
								</Link>
							</u>
						</ListItem>
						<ListItem>
							<u>
								<Link
									href="https://www.aifb.kit.edu/web/Tobias_K%C3%A4fer"
									target="_blank"
								>
									Tobias Käfer
								</Link>
							</u>
							,{" "}
							<u>
								<Link href="http://www.aifb.kit.edu/">
									Institute AIFB, Karlsruhe Institute of Technology (KIT), Germany
								</Link>
							</u>
						</ListItem>
					</UnorderedList>

					<Heading size="xl">References</Heading>
					<UnorderedList pl={6}>
						<ListItem>
							[1] Acosta, M., and Vidal, M.-E. Networks of linked data eddies: An
							adaptive web query processing engine for rdf data. In The Semantic Web -
							ISWC 2015 (Oktober2015), International Semantic Web Conference,
							Springer, pp. 111–127.
						</ListItem>
						<ListItem>
							[2] Heling, L., and Acosta, M. Cost- and robustness-based query
							optimization for linked data fragments. In The Semantic Web – ISWC 2020
							(Cham, 2020), J. Z.Pan, V. Tamma, C. d’Amato, K. Janowicz, B. Fu, A.
							Polleres, O. Seneviratne, and L. Kagal, Eds., Springer International
							Publishing, pp. 238–257.
						</ListItem>
						<ListItem>
							[3] Lewis, J. R., and Sauro, J. The factor structure of the system
							usability scale. In Human Centered Design(Berlin, Heidelberg, 2009), M.
							Kurosu, Ed., Springer BerlinHeidelberg, pp. 94–103.
						</ListItem>
						<ListItem>
							[4] Verborgh, R., Hartig, O., De Meester, B., Haesendonck, G., De
							Vocht,L., Vander Sande, M., Cyganiak, R., Colpaert, P., Mannens, E., and
							Van de Walle, R.Querying datasets on the web with high availability. In
							The Semantic Web – ISWC 2014 (Cham, 2014), P. Mika, T. Tudorache, A.
							Bernstein,C. Welty, C. Knoblock, D. Vrandečić, P. Groth, N. Noy, K.
							Janowicz, and C. Goble,Eds., Springer International Publishing, pp.
							180–196.
						</ListItem>
						<ListItem>
							[5] Lewis, J. R., and Sauro, J., The factor structure of the system
							usability scale. In Human Centered Design(Berlin, Heidelberg, 2009), M.
							Kurosu, Ed., Springer BerlinHeidelberg, pp. 94–103
						</ListItem>
					</UnorderedList>
				</VStack>
			</>
		);
	}
}

export default AboutPage;
