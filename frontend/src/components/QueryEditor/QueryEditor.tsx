import React, { useState, useEffect, useRef } from "react";
import {
	Button,
	InputGroup,
	InputLeftAddon,
	Input,
	Flex,
	Select,
	FormLabel,
	FormControl,
	Heading,
} from "@chakra-ui/react";

import SourcesEditor from "components/SourcesEditor/SourcesEditor";
import { DEFAULT_QUERY } from "../../constants/queryEditor";

import { formatQuery } from "../../utils/utils";

import Alert from "components/Alert/Alert";

import "./QueryEditor.scss";

import "@triply/yasqe/build/yasqe.min.css";

import Yasqe from "@triply/yasqe";
import { AlertContent } from "components/HoCs/withAlert";

const MODE_EDIT = "edit";
const MODE_VIEW = "view";

const DEFAULT_SOURCE = "tpf@http://fragments.dbpedia.org/2014/en";

const SUPPORTED_QUERY_TYPES = ["SELECT"];

interface IPropsQueryEditor {
	taskId?: string;
}

interface IPropsEditMode extends IPropsQueryEditor {
	mode: "edit";
	querySubmitted: boolean;
	resetSubmition(): void;
	submitQuery(query: string, sources: string[], optimizerName: string, queryName?: string): void;
}

interface IPropsViewMode extends IPropsQueryEditor {
	mode: "view";
	query: string;
}

const QueryEditor = (props: IPropsEditMode | IPropsViewMode) => {
	const yasqeRef = useRef(null);

	const yasqeId = props.taskId ? `yasqe-${props.taskId}` : `yasqe`;

	const [yasqe, setYasqe] = useState<Yasqe>();

	const [forbiddenKeywordUsed, setForbiddenKeywordUsed] = useState(false);

	const [error, setError] = useState<AlertContent>();

	const [queryName, setQueryName] = useState("");

	const [optimizer, setOptimizerName] = useState("left-deep");

	const [sources, setSources] = useState<Set<string>>(() => {
		const storedSources = localStorage.getItem("sources");
		if (storedSources && typeof storedSources === "string" && storedSources.length > 2) {
			return new Set(JSON.parse(storedSources));
		}
		return new Set([DEFAULT_SOURCE]);
	});

	useEffect(() => {
		// Manually remove last query and results.
		// The persistencyExpire field didn't work as described here https://triply.cc/docs/yasgui-api
		// Except in this issue the renewal of cache was not further discussed https://github.com/TriplyDB/Yasgui/issues/151
		localStorage.removeItem("yasqe__query");
		localStorage.removeItem("prefixes");

		const query = (() => {
			if ("query" in props) {
				return props.query;
			}
			const queryInStorage = localStorage.getItem("query");
			if (queryInStorage && typeof queryInStorage === "string") {
				return queryInStorage;
			}
			return DEFAULT_QUERY;
		})();

		const yasqeElement = document.getElementById(yasqeId);
		if (!yasqeElement) {
			return;
		}

		const yasqeInstance = new Yasqe(yasqeElement, {
			readOnly: props.mode === MODE_VIEW,
		});
		yasqeInstance.setSize(null, 200);

		yasqeInstance.setValue(query);

		setYasqe(yasqeInstance);

		// Check for forbidden keywords on change & on initial load
		handleOnQueryChange(yasqeInstance.getValue(), yasqeInstance);
		yasqeInstance.on("change", () => {
			handleOnQueryChange(yasqeInstance.getValue(), yasqeInstance);
		});

		return () => {};
	}, []);

	if ("querySubmitted" in props) {
		// Disable editing the query if the query has already been submitted.
		useEffect(() => {
			if (yasqe) {
				yasqe.setOption("readOnly", props.querySubmitted);
			}
		}, [props.querySubmitted]);
	}

	const showErrorMessage = () => {
		if (error && error.title && error.msg) {
			return <Alert title={error.title} description={error.msg} status="error" />;
		}
		return null;
	};

	const handleOnQueryChange = (query, yasqeInstance) => {
		const keywordCheck = checkForForbiddenKeywords(query);
		if (keywordCheck.forbiddenKeywordFound) {
			document.getElementsByClassName("CodeMirror")[0].classList.add("forbidden-keyword");
			setForbiddenKeywordUsed(true);
			setError({
				title: "Operator not supported",
				msg: `The operator ${keywordCheck.keyword} is currently not supported.`,
				status: "error",
			});
		} else {
			document.getElementsByClassName("CodeMirror")[0].classList.remove("forbidden-keyword");
			setForbiddenKeywordUsed(false);
			setError(undefined);
		}

		if (!yasqeInstance.queryValid) {
			setForbiddenKeywordUsed(true);
		}
	};

	const checkForForbiddenKeywords = (query) => {
		const FORBIDDEN_KEYWORDS = [
			"UNION",
			"OPTIONAL",
			"FILTER",
			"GROUP",
			"DESC",
			"BIND",
			"ASC",
			"BOUND",
			"REGEX",
			"ISURI",
			"ISBLANK",
			"ISLITERAL",
			"LANG",
			"DATATYPE",
			"STR",
			"GROUPBY",
			"COUNT",
			"ASK",
			"CONSTRUCT",
			"DESCRIBE",
		];

		for (const keyword of FORBIDDEN_KEYWORDS) {
			query = query.toUpperCase();
			query = query.replaceAll(/\s+/g, " ");
			query = query.replaceAll(`${keyword}{`, `${keyword} {`);
			query = query.replaceAll(`}${keyword}`, `} ${keyword}`);
			query = query.replaceAll(`.${keyword}`, `. ${keyword}`);
			query = query.replaceAll(`\n${keyword}`, `\n ${keyword}`);
			query = query.replaceAll(`${keyword}\n`, `${keyword} \n`);
			query = ` ${query} `;

			const keywordwithspaces = ` ${keyword} `;
			if (query.includes(keywordwithspaces)) {
				return { forbiddenKeywordFound: true, keyword };
			}
		}

		return { forbiddenKeywordFound: false, keyword: null };
	};

	const handleSubmit = () => {
		if (isQueryValid() && areSourcesValid() && yasqe && props.mode === MODE_EDIT) {
			const formattedQuery = formatQuery(yasqe.getValue());
			console.log("Formatted Query:");
			console.log(formattedQuery);

			yasqe.setValue(formattedQuery);

			localStorage.setItem("query", formattedQuery);
			props.submitQuery(
				formattedQuery,
				[...sources],
				optimizer,
				queryName !== "" ? queryName : undefined
			);
		}
	};

	const areSourcesValid = () => {
		if (!sources) {
			setError({
				title: "Invalid sources",
				msg: "No sources provided",
				status: "error",
			});
			return false;
		}
		setError(undefined);
		return true;
	};

	const isQueryValid = () => {
		if (!yasqe) {
			return false;
		}
		const title = "Invalid query";

		if (yasqe.getValue() === "") {
			setError({
				title: title,
				msg: "You provided no query.",
				status: "error",
			});
			return false;
		}

		// Currently only SELECT supported
		if (!SUPPORTED_QUERY_TYPES.includes(yasqe.getQueryType())) {
			setError({
				title: title,
				msg: `Currently only the query types ${SUPPORTED_QUERY_TYPES.join(
					", "
				)}  are supported.`,
				status: "error",
			});
			return false;
		}

		// Check if syntax is correct
		if (!yasqe.queryValid) {
			setError({
				title: title,
				msg: "Invalid query Syntax. Check editor for invalid lines",
				status: "error",
			});
			return false;
		}

		setError(undefined);
		return true;
	};

	return (
		<>
			{props.mode === MODE_EDIT ? (
				<>
					<Heading as="h1" size="lg" mb="16px">
						Query Editor
					</Heading>
					{showErrorMessage()}

					<InputGroup>
						<InputLeftAddon borderRadius={0}>Query name</InputLeftAddon>

						<Input
							borderRadius={0}
							onChange={(evt) => setQueryName(evt.target.value)}
							type="text"
							placeholder="Provide query name for easier identification (optional)"
							mb="4"
						/>
					</InputGroup>
					<SourcesEditor
						sources={sources}
						setSources={setSources}
						querySubmitted={props.querySubmitted}
					/>
					<div id={yasqeId} ref={yasqeRef} />
					<Flex>
						<FormControl id="optimizer">
							<FormLabel>Select Optimizer</FormLabel>
							<Select
								size="md"
								mb={4}
								onChange={(evt) => setOptimizerName(evt.target.value)}
								disabled={
									sources.size < 1 || props.querySubmitted || forbiddenKeywordUsed
								}
							>
								<option value="left-deep" selected>
									Left-Linear
								</option>
								<option value="nLDE">nLDE</option>
								<option value="CROP">CROP</option>
							</Select>
						</FormControl>
					</Flex>
					<Flex>
						<Button
							disabled={
								sources.size < 1 || props.querySubmitted || forbiddenKeywordUsed
							}
							onClick={handleSubmit}
						>
							Get query plan
						</Button>
						<Button
							ml={2}
							display={{ base: props.querySubmitted ? "block" : "none" }}
							onClick={props.resetSubmition}
						>
							Reset
						</Button>
					</Flex>
				</>
			) : (
				<>
					<div id={yasqeId} ref={yasqeRef} />
				</>
			)}
		</>
	);
};

export default QueryEditor;
