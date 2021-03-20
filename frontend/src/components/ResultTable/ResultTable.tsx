import React, { useState, useEffect, useRef } from "react";

import "@triply/yasr/build/yasr.min.css";
import Yasr from "@triply/yasr";
import { logger } from "utils/logger";

function ResultTable(props) {
	const yasrRef = useRef(null);

	const [yasr, setYasr] = useState<Yasr>();

	const updateResults = (yasrInstance) => {
		if (props.results) {
			yasrInstance.setResponse({
				data: props.results,
				contentType: "application/sparql-results+json",
				status: 200,
			});
		}
	};

	useEffect(() => {
		localStorage.removeItem("yasr__response");

		if (document.getElementById("yasr")) {
			const yasrInstance = new Yasr(document.getElementById("yasr") as HTMLElement, {});
			updateResults(yasrInstance);
			setYasr(yasrInstance);
		}
		return () => {};
	}, []);

	// Call this method each time the provided results change
	useEffect(() => {
		if (yasr && props.results) {
			logger(props.results);
			updateResults(yasr);
		}
	}, [props.results]);

	// const isFetchingYetNoResults = () => {
	//   logger('evaluating')
	//   const {status, results} = props;
	//   const resultExists = results.results && results.results.bindings && results.results.bindings.length === 0
	//   logger(status === "pending" && resultExists)
	//   return (status === "pending" && resultExists);
	// };

	return (
		<>
			<div id="yasr" ref={yasrRef} />
		</>
	);
}

export default ResultTable;
