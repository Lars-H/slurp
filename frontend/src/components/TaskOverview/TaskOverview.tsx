import React, {useEffect, useState} from "react";
import PropTypes from "prop-types";
import {
	Accordion,
	AccordionItem,
	AccordionButton,
	Box,
	AccordionIcon,
	AccordionPanel,
	Stack,
	Flex,
	Badge,
	ExpandedIndex,
	Heading,
} from "@chakra-ui/react";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";
import MetaBadges from "components/MetaBadges/MetaBadges";
import QueryEditor from "components/QueryEditor/QueryEditor";
import ResultTable from "components/ResultTable/ResultTable";
import {timeConverter} from "utils/utils";
import BinaryTree from "utils/DataStructures/binaryTree";
import {ITaskPageDataResponse} from "interface/ITaskPageDataResponse";

interface ITaskOverviewProps extends Partial<ITaskPageDataResponse> {
	splitView: boolean;
	extendedItems: ExpandedIndex;
	updateExtendedItems: (extendedItems: ExpandedIndex) => void;
}

const TaskOverview = (props: ITaskOverviewProps) => {
	const [cyPlan, setCyPlan] = useState();

	useEffect(() => {
		transformExecutionPlanForCy();
	}, [props.plan]);

	useEffect(() => {
		console.log(props._id);
		console.log(props.extendedItems);
	}, [props.extendedItems]);

	const transformExecutionPlanForCy = () => {
		if (props.plan && props.query) {
			const tree = new BinaryTree();
			tree.buildTreeFromExecutionPlan(props.plan, props.query);
			const treeElements = tree.getElements();

			setCyPlan(treeElements);
		} else {
			console.log("Error. Execution plan or Query not available");
		}
	};

	const noResultsAfterQueryFinished =
		props.status &&
		props.sparql_results &&
		["done", "timeout", "failed"].includes(props.status) &&
		props.sparql_results.results.bindings.length === 0;

	return (
		<>
			{props.query && props._id && props.status && (
				<Accordion
					allowToggle
					index={props.extendedItems}
					allowMultiple
					width={props.splitView ? "50%" : "100%"}
					onChange={(extendedItems) => props.updateExtendedItems(extendedItems)}
				>

					<Box pl="16px" mb="8px">
						<Heading as="h1" size="md" mb="16px">
							Task {props._id}
						</Heading>
						{props.query_name && (
							<Heading mr="1" size="sm">
								Name: {props.query_name}
							</Heading>
						)}
					</Box>


					<AccordionItem>
						<AccordionButton>
							<Box flex="1" textAlign="left">
								Information
							</Box>
							<AccordionIcon />
						</AccordionButton>

						<AccordionPanel pb={4}>
							<Stack shouldWrapChildren spacing="32px">
								<MetaBadges
									status={props.status}
									resultCount={props.result_count}
									requests={props.requests}
									tDelta={props.t_delta}
									showRequestHint={true}
									tStart={timeConverter(props.t_start)}
									tEnd={props.t_end && timeConverter(props.t_end)}
								/>
								<Flex wrap="wrap" mt="-5">
									{props.sources &&
										props.sources.map((el) => {
											return <Badge key={el}>{el}</Badge>;
										})}
								</Flex>
							</Stack>
						</AccordionPanel>
					</AccordionItem>

					<AccordionItem>
						<AccordionButton>
							<Box flex="1" textAlign="left">
								Query
							</Box>
							<AccordionIcon />
						</AccordionButton>
						<AccordionPanel pb={4}>
							<QueryEditor mode="view" query={props.query} taskId={props._id} />
						</AccordionPanel>
					</AccordionItem>

					<AccordionItem>
						<AccordionButton>
							<Box flex="1" textAlign="left">
								Execution Plan
							</Box>
							<AccordionIcon />
						</AccordionButton>
						<AccordionPanel pb={4}>
							<ColoredExecutionPlanner mode="view" suggestedExecutionPlan={cyPlan} />
						</AccordionPanel>
					</AccordionItem>

					{noResultsAfterQueryFinished ? (
						<AccordionItem>
							<Box
								height="40px"
								borderBottomColor="rgb(226, 232, 240)"
								padding="8px 16px"
								userSelect="none"
							>
								No Results
							</Box>
						</AccordionItem>
					) : (
						<AccordionItem>
							<AccordionButton>
								<Box flex="1" textAlign="left">
									Results
								</Box>
								<AccordionIcon />
							</AccordionButton>
							<AccordionPanel pb={4}>
								<ResultTable
									results={props.sparql_results}
									status={props.status}
									taskId={props._id}
								/>
							</AccordionPanel>
						</AccordionItem>
					)}
				</Accordion>
			)}
		</>
	);
};

export default TaskOverview;
