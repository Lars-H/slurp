import React, { useState, useEffect, useRef } from "react";

import "@triply/yasr/build/yasr.min.css";
import Yasr from "@triply/yasr";

interface IResultTableProps {
	results: any;
	status: any;
	taskId: string;
}

function ResultTable(props) {
	const yasrRef = useRef(null);

	const yasrId = props.taskId ? `yasr-${props.taskId}` : `yasr`;

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

		if (document.getElementById(yasrId)) {
			const yasrInstance = new Yasr(document.getElementById(yasrId) as HTMLElement, {});
			updateResults(yasrInstance);
			setYasr(yasrInstance);
		}
		return () => {};
	}, []);

	// Call this method each time the provided results change
	useEffect(() => {
		if (yasr && props.results) {
			console.log(props.results);
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
			<div id={yasrId} ref={yasrRef} />
		</>
	);
}

export default ResultTable;
