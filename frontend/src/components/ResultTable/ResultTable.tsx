import Yasr from "@triply/yasr";
import "@triply/yasr/build/yasr.min.css";

import React, { useState, useEffect, useRef } from "react";
import { TaskStatus } from "interface/ITaskPageDataResponse";

interface IResultTableProps {
	results: any;
	status: TaskStatus;
	taskId: string;
	opened: boolean;
}

function ResultTable(props: IResultTableProps) {
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

	useEffect(() => {
		// Workaround: rerender the table to show the borders.
		// Sessionstorage is cleared because it contains vertical border starting points.
		// Some cells might yet still be hidden if resolution is too low.
		sessionStorage.clear();
		props.opened && yasr && yasr.draw();
	}, [props.opened]);

	return (
		<>
			<div id={yasrId} ref={yasrRef} />
		</>
	);
}

export default ResultTable;
