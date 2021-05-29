import React from "react";
import { ExpandedIndex } from "@chakra-ui/accordion";
import { Box, Button, Flex, Heading, Select, Tooltip } from "@chakra-ui/react";
import { ITaskPageDataResponse } from "interface/ITaskPageDataResponse";
import { useState } from "react";
import TaskOverview, { OverviewElements } from "./TaskOverview";

interface TwoTasksComparisonProps {
	first: ITaskPageDataResponse;
	others: ITaskPageDataResponse[];
	leaveComparison: () => void;
}

const TwoTasksComparison = (props: TwoTasksComparisonProps) => {
	const [extendedItems, setExtendedItems] = useState<ExpandedIndex>([0]);
	const [comparand, setComparand] = useState<ITaskPageDataResponse>(props.others[0]);

	const [heights, setHeights] = useState<Record<OverviewElements, number>>({
		heading: 0,
		info: 0,
		query: 0,
		plan: 0,
		results: 0,
	});

	const updateSplitViewHeights = (taskOverviewHeights: Record<OverviewElements, number>) => {
		const newHeights = { ...heights };
		let updates = 0;

		Object.entries(taskOverviewHeights).forEach(([key, value]) => {
			if (heights[key] < value) {
				console.log(key, value);
				newHeights[key] = value;
				updates += 1;
			}
		});

		if (updates > 0) {
			setHeights(newHeights);
		}
	};

	const compareWithExecutionPlan = (id: string) => {
		const comparandIndex = props.others.findIndex((el) => el._id === id);

		if (comparandIndex !== -1) {
			setComparand(props.others[comparandIndex]);
			setExtendedItems([0]);
		}
	};

	const getExecutionTime = (task: ITaskPageDataResponse) => {
		const bindings = task.sparql_results.results.bindings;
		if(bindings.length === 0) {
			throw new Error('No bindings');
		}
		return bindings[bindings.length - 1]['_trace_'].value;
	}

	const getResultCount = (task: ITaskPageDataResponse): number => {
		return task.sparql_results.results.bindings.length;
	}

	const getPlotXandYLimits = (first: ITaskPageDataResponse, second: ITaskPageDataResponse): {x: number, y: number} => {
		return {
			x: Math.max(getExecutionTime(first), getExecutionTime(second)),
			y: Math.max(getResultCount(first), getResultCount(comparand)),
		}
	}

	return (
		<>
			<Flex justifyContent="center" mb="16px">
				<Heading as="h1" size="md" marginY="auto" pl="16px" width="50%">
					Task {props.first._id}
				</Heading>
				<Box width="50%" pl="16px">
					<Flex marginY="auto">
						<Select
							defaultValue={props.others[0]._id}
							style={{ fontWeight: 700, fontSize: "1.25rem", lineHeight: 1.2 }}
							onChange={(evt) => compareWithExecutionPlan(evt.target.value)}
						>
							{props.others.map((el) => {
								return (
									<option value={el._id} key={el._id}>
										Task {el._id}
									</option>
								);
							})}
						</Select>
						<Button ml={2} onClick={props.leaveComparison}>
							Hide
						</Button>
					</Flex>
				</Box>
			</Flex>

			{(props.first.query_name || comparand.query_name) && (
				<Flex marginBottom="16px">
					<Heading mr="1" size="sm" width="50%" pl="16px">
						{props.first.query_name && `Name: ${props.first.query_name}`}
					</Heading>
					<Heading mr="1" size="sm" width="50%" marginLeft="16px" pl="24px">
						{comparand.query_name && `Name: ${comparand.query_name}`}
					</Heading>
				</Flex>
			)}

			<Flex>
				<Box width="50%">
					<TaskOverview
						{...props.first}
						splitView={true}
						extendedItems={extendedItems}
						updateExtendedItems={setExtendedItems}
						updateHeights={updateSplitViewHeights}
						plotLimits={getPlotXandYLimits(props.first, comparand)}
						heights={heights}
					/>
				</Box>
				<Box width="50%" marginLeft="32px">
					{comparand && (
						<>
							<TaskOverview
								{...comparand}
								splitView={true}
								extendedItems={extendedItems}
								updateExtendedItems={setExtendedItems}
								updateHeights={updateSplitViewHeights}
								plotLimits={getPlotXandYLimits(props.first, comparand)}
								heights={heights}
							/>
						</>
					)}
				</Box>
			</Flex>
		</>
	);
};

export default TwoTasksComparison;
