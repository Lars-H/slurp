import React from "react";
import PropTypes from "prop-types";
import { Badge, Spinner } from "@chakra-ui/react";

const StatusBadge = (props) => {
	const { status } = props;
	let colorScheme: string;

	if (status === "pending") {
		colorScheme = "orange";
	} else if (status === "done") {
		colorScheme = "green";
	} else if (status === "queue") {
		colorScheme = "teal";
	} else {
		colorScheme = "red";
	}

	return (
		<Badge colorScheme={colorScheme}>
			{status}
			{status === "pending" && <Spinner size="xs" ml="1" />}
		</Badge>
	);
};

StatusBadge.propTypes = {
	status: PropTypes.string.isRequired,
};

export default StatusBadge;
