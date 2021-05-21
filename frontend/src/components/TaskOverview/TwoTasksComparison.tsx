import React from "react";
import { ExpandedIndex } from "@chakra-ui/accordion";
import { Box, Flex } from "@chakra-ui/react";
import { ITaskPageDataResponse } from "interface/ITaskPageDataResponse";
import { useState } from "react";
import TaskOverview, { OverviewElements } from "./TaskOverview";

interface TwoTasksComparisonProps {
	first: ITaskPageDataResponse;
	second: ITaskPageDataResponse;
}

const TwoTasksComparison = (props: TwoTasksComparisonProps) => {
	const [extendedItems, setExtendedItems] = useState<ExpandedIndex>([]);
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
				newHeights[key] = value;
				updates += 1;
			}
		});

		if (updates > 0) {
			setHeights(newHeights);
		}
	};

	return (
		<Flex>
			<TaskOverview
				{...props.first}
				splitView={true}
				extendedItems={extendedItems}
				updateExtendedItems={setExtendedItems}
				updateHeights={updateSplitViewHeights}
				heights={heights}
			/>
			<Box marginLeft="32px"></Box>

			{/* TODO: REFACTOR. Just a workaround for now: to avoid multiple setState calls, 
            render second first component after second has been rendered */}
			{heights.heading > 0 && (
				<TaskOverview
					{...props.second}
					splitView={true}
					extendedItems={extendedItems}
					updateExtendedItems={setExtendedItems}
					updateHeights={updateSplitViewHeights}
					heights={heights}
				/>
			)}
		</Flex>
	);
};

export default TwoTasksComparison;
