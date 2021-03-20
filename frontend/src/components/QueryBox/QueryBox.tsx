import React from "react";
// import PropTypes from "prop-types";
import { Box, Text, Code, Collapse, Button, Flex } from "@chakra-ui/react";
import { ChevronUpIcon, ChevronDownIcon } from "@chakra-ui/icons";
import MetaBadges from "components/MetaBadges/MetaBadges";
import { withRouter, RouteComponentProps } from "react-router-dom";
import PropTypes from "prop-types";
import { ProcessingQueryStatusTypes } from "interface/ITaskPageDataResponse";

interface QueryBoxProps {
	key: string;
	id: string;
	status: ProcessingQueryStatusTypes;
	resultCount: number;
	tDelta: number;
	requests: number;
	tStart: string;
	query: string;
	queryName?: string;
}

const QueryBox = (props: QueryBoxProps & RouteComponentProps) => {
	const { id, status, resultCount, tStart, query, tDelta, requests, queryName } = props;

	const forwardToTaskPage = () => {
		props.history.push(`/task/${id}`);
	};

	const [show, setShow] = React.useState(false);

	const handleToggle = () => setShow(!show);

	return (
		<Box
			style={{ cursor: "pointer" }}
			border="1px"
			borderColor="gray.200"
			borderRadius="md"
			boxShadow="md"
			p="3"
			w="100%"
			onClick={handleToggle}
		>
			<Flex justify="space-between" wrap="wrap">
				<Text fontWeight="bold">
					{queryName && <span style={{ textDecoration: "underline" }}>{queryName}:</span>}{" "}
					<span> {id}</span>
				</Text>
				<MetaBadges
					status={status}
					resultCount={resultCount}
					tDelta={tDelta}
					requests={requests}
					tStart={tStart}
				/>
			</Flex>
			<Collapse startingHeight={40} in={show}>
				<Code fontSize="sm" mt="1" mb="1" w="100%" style={{ whiteSpace: "pre-wrap" }}>
					{query}
				</Code>
			</Collapse>
			<Button size="sm" onClick={forwardToTaskPage} mt="1rem">
				Details
			</Button>
			{show ? (
				<ChevronUpIcon float="right" position="relative" right="0" />
			) : (
				<ChevronDownIcon float="right" position="relative" right="0" />
			)}
		</Box>
	);
};

// TODO: FIX
QueryBox.propTypes = {
	id: PropTypes.string.isRequired,
	status: PropTypes.oneOf<ProcessingQueryStatusTypes>(["done", "timeout", "pending"]).isRequired,
	resultCount: PropTypes.number.isRequired,
	tStart: PropTypes.string.isRequired,
	query: PropTypes.string.isRequired,
	tDelta: PropTypes.number.isRequired,
	requests: PropTypes.number.isRequired,
	queryName: PropTypes.string,
};

export default withRouter(QueryBox);
