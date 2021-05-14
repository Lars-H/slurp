import React from "react";
import { Box, Badge, HStack, Tooltip } from "@chakra-ui/react";
import StatusBadge from "components/QueryBox/StatusBadge";

const MetaBadges = (props) => {
	return (
		<HStack wrap="wrap">
			<StatusBadge status={props.status} />
			{(props.resultCount !== null || props.resultCount !== undefined) && (
				<Badge colorScheme="yellow">Results: {props.resultCount}</Badge>
			)}
			<Badge colorScheme="purple">Runtime: {props.tDelta.toFixed(2)} Sec</Badge>
			{props.requests === 0 && props.showRequestHint ? (
				<Tooltip label="In case of a Timeout, the number of requests are not counted">
					<Badge colorScheme="purple" pr="0">
						<HStack spacing="2px">
							<Box>Requests: {props.requests}</Box>
							<Box
								textAlign="center"
								w="16px"
								fontWeight="700"
								background="#CBD5E0"
								ml="60px"
							>
								?
							</Box>
						</HStack>
					</Badge>
				</Tooltip>
			) : (
				<Badge colorScheme="purple">Requests: {props.requests}</Badge>
			)}
			<Badge colorScheme="blue">Start: {props.tStart}</Badge>
			{props.tEnd && <Badge colorScheme="teal">End: {props.tEnd}</Badge>}
		</HStack>
	);
};

export default MetaBadges;
