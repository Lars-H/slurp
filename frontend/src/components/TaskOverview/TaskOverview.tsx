import React, { useEffect, useState } from "react";
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
} from "@chakra-ui/react";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";
import MetaBadges from "components/MetaBadges/MetaBadges";
import QueryEditor from "components/QueryEditor/QueryEditor";
import ResultTable from "components/ResultTable/ResultTable";
import { timeConverter } from "utils/utils";
import BinaryTree from "utils/DataStructures/binaryTree";
import { ITaskPageDataResponse } from "interface/ITaskPageDataResponse";

interface ITaskOverviewProps extends Partial<ITaskPageDataResponse> {}

const TaskOverview = (props: ITaskOverviewProps) => {
	const [cyPlan, setCyPlan] = useState();

	useEffect(() => {
		// TODO: Check here?
		transformExecutionPlanForCy();
	}, [props.plan]);

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

	return (
		<>
			{props.query && props._id && (
				<Accordion defaultIndex={[0, 3]} allowMultiple>
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
				</Accordion>
			)}
		</>
	);
};

export default TaskOverview;
