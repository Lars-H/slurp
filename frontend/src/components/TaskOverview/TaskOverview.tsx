/* eslint-disable react/react-in-jsx-scope */
import { useEffect, useRef, useState } from "react";
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
	AccordionProps,
} from "@chakra-ui/react";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";
import MetaBadges from "components/MetaBadges/MetaBadges";
import QueryEditor from "components/QueryEditor/QueryEditor";
import ResultTable from "components/ResultTable/ResultTable";
import { timeConverter } from "utils/utils";
import BinaryTree from "utils/DataStructures/binaryTree";
import { ITaskPageDataResponse, TaskStatus } from "interface/ITaskPageDataResponse";
import { NoResultsAccordionItem } from "./NoResultsAccordionItem";

export type OverviewElements = "heading" | "info" | "query" | "plan" | "results";

// enum OverviewElements {
// 	heading = 'heading',
// 	info = 'info',
// 	query = 'query',
// 	plan = 'plan',
// 	results = 'results',
// }
interface ITaskOverviewProps extends ITaskPageDataResponse {
	splitView: boolean;
	extendedItems?: ExpandedIndex;
	updateExtendedItems?: (extendedItems: ExpandedIndex) => void;
	heights?: Record<OverviewElements, number>;
	updateHeights?: any;
}

const TaskOverview = (props: ITaskOverviewProps) => {
	const [cyPlan, setCyPlan] = useState();
	const [heights, setHeights] = useState<Record<OverviewElements, number>>({
		heading: 0,
		info: 0,
		query: 0,
		plan: 0,
		results: 0,
	});

	const references: Record<OverviewElements, React.RefObject<HTMLDivElement>> = {
		heading: useRef<HTMLDivElement>(null),
		info: useRef<HTMLDivElement>(null),
		query: useRef<HTMLDivElement>(null),
		plan: useRef<HTMLDivElement>(null),
		results: useRef<HTMLDivElement>(null),
	};

	useEffect(() => {
		transformExecutionPlanForCy();
	}, [props.plan]);

	useEffect(() => {
		if (props.heights) {
			setHeights(props.heights);
		}
	}, [props.heights]);

	useEffect(() => {
		const currentHeights = {};
		Object.keys(references).forEach((key) => {
			if (references[key as OverviewElements].current) {
				currentHeights[key] = references[key as OverviewElements].current?.offsetHeight;
			}
		});

		props.updateHeights && props.updateHeights(currentHeights);
	});

	const transformExecutionPlanForCy = () => {
		const tree = new BinaryTree();
		tree.buildTreeFromExecutionPlan(props.plan, props.query);
		const treeElements = tree.getElements();

		setCyPlan(treeElements);
	};

	const isResultTableOpened = () => {
		if (Array.isArray(props.extendedItems)) {
			return props.extendedItems.includes(3);
		} else {
			return props.extendedItems === 3;
		}
	};

	const getAccordionProps = () => {
		const accordionProps: AccordionProps = {
			allowToggle: true,
			allowMultiple: true,
		};

		if ("extendedItems" in props && "updateExtendedItems" in props) {
			Object.assign(accordionProps, {
				onChange: (extendedItems) => {
					props.updateExtendedItems && props.updateExtendedItems(extendedItems);
				},
				index: props.extendedItems,
			});
		} else {
			Object.assign(accordionProps, {
				defaultIndex: [0, 3],
			});
		}
		return accordionProps;
	};

	const noResultsAfterQueryFinished =
		[TaskStatus.done, TaskStatus.timeout, TaskStatus.failed].includes(props.status) &&
		props.sparql_results.results.bindings.length === 0;

	return (
		<Accordion {...getAccordionProps()}>
			<AccordionItem>
				<AccordionButton>
					<Box flex="1" textAlign="left">
						Information
					</Box>
					<AccordionIcon />
				</AccordionButton>
				<AccordionPanel pb={4} ref={references.info} minHeight={heights.info}>
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
				<AccordionPanel pb={4} ref={references.query} minHeight={heights.query}>
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
				<AccordionPanel pb={4} ref={references.plan} minHeight={heights.plan}>
					<ColoredExecutionPlanner mode="view" suggestedExecutionPlan={cyPlan} />
				</AccordionPanel>
			</AccordionItem>

			{noResultsAfterQueryFinished ? (
				<NoResultsAccordionItem />
			) : (
				<AccordionItem>
					<AccordionButton>
						<Box flex="1" textAlign="left">
							Results
						</Box>
						<AccordionIcon />
					</AccordionButton>
					<AccordionPanel pb={4} ref={references.results} minHeight={heights.results}>
						<ResultTable
							results={props.sparql_results}
							status={props.status}
							taskId={props._id}
							opened={isResultTableOpened()}
						/>
					</AccordionPanel>
				</AccordionItem>
			)}
		</Accordion>
	);
};

export default TaskOverview;
