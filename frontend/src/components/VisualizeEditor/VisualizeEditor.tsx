import React, { useState } from "react";

import { Controlled as CodeMirror } from "react-codemirror2";
import "codemirror/lib/codemirror.css";
import "codemirror/theme/material.css";
// import jsonlint from "jsonlint";
// This import is for the language syntax highlighting.
import "codemirror/addon/lint/lint.css";
import "codemirror/addon/hint/show-hint.css";
import "codemirror/mode/javascript/javascript.js";
import "codemirror/addon/lint/javascript-lint";
import "codemirror/addon/lint/lint";
import "codemirror/addon/lint/lint.js";
import "codemirror/addon/hint/javascript-hint";
import { Box, Button, Heading } from "@chakra-ui/react";
import { JSHINT } from "jshint";
import { AlertContent, renderAlert } from "components/HoCs/withAlert";

declare const window: any;
window.JSHINT = JSHINT;

interface IVisualizeEditorProps {
	visualizePlan(execPlan: string): void;
}

const VisualizeEditor = (props: IVisualizeEditorProps) => {
	const [execPlan, setExecPlan] = useState<string>("");
	const [alert, setAlert] = useState<AlertContent | null>(null);

	const onVisualizeClick = () => {
		if (!execPlan) {
			setAlert({ title: "Invalid Plan", msg: "No plan was provided", status: "error" });
			return;
		}

		let planJSON: any;
		try {
			planJSON = JSON.parse(execPlan);
		} catch (err) {
			setAlert({
				title: "Invalid Plan",
				msg: "Plan is not in valid JSON format",
				status: "error",
			});
			return;
		}
		props.visualizePlan(planJSON);
		setAlert(null);
	};

	return (
		<Box>
			<Heading as="h1" size="lg" mb="16px">
				Enter the plan you want to visualize
			</Heading>
			{renderAlert(alert)}
			<Box border="1px solid #d1d1d1">
				<CodeMirror
					value={execPlan}
					options={{
						gutters: ["CodeMirror-lint-markers"],
						mode: "application/json",
						theme: "xq-light",
						// styleActiveLine: true,
						lineNumbers: true,
						lint: true,
					}}
					onBeforeChange={(editor, data, value: string) => {
						setExecPlan(value);
					}}
				/>
			</Box>
			<Button onClick={onVisualizeClick} mt="16px">
				Visualize
			</Button>
		</Box>
	);
};

export default VisualizeEditor;
