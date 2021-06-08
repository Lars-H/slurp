import React from "react";
import {Box, Badge, HStack, Tooltip, Flex, Grid, Wrap, WrapItem} from "@chakra-ui/react";
import StatusBadge from "components/QueryBox/StatusBadge";

const MetaBadges = (props) => {
	console.log(props.resultCount)
	return (
		<Wrap wrap="wrap">
			<WrapItem>
				<StatusBadge status={props.status} />
			</WrapItem>

			<WrapItem>
				<Badge colorScheme="yellow">Results: {props.resultCount ?? '-'}</Badge>
			</WrapItem>

			<WrapItem>
				<Badge colorScheme="purple">Runtime: {props.tDelta.toFixed(2)} Sec</Badge>
			</WrapItem>

			{props.requests === 0 && props.showRequestHint ? (
				<WrapItem>
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
				</WrapItem>
			) : (
				<WrapItem>
					<Badge colorScheme="purple">Requests: {props.requests}</Badge>
				</WrapItem>
			)}

			<WrapItem>
				<Badge colorScheme="blue">Start: {props.tStart}</Badge>
			</WrapItem>

			{props.tEnd && (
				<WrapItem>
					<Badge colorScheme="teal">End: {props.tEnd}</Badge>
				</WrapItem>
			)}
		</Wrap>
	);
};

export default MetaBadges;
