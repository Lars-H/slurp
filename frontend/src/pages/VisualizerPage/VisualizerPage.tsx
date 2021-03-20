import React, { Component, useState } from "react";
import { Box, Button, Flex, Heading } from "@chakra-ui/react";
import VisualizeEditor from "components/VisualizeEditor/VisualizeEditor";
import BinaryTree from "utils/DataStructures/binaryTree";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";

interface Props {}

const VisualizerPage = (props: Props) => {
	const [cyElements, setCyElements] = useState();

	const visualizePlan = (execPlan?: any) => {
		if (cyElements) {
			setCyElements(undefined);
		}
		if (execPlan) {
			const tree = new BinaryTree();
			tree.buildTreeFromExecutionPlan(execPlan, "");
			const treeElements = tree.getElements();
			setCyElements(treeElements);
		}
	};

	return (
		<Box>
			<VisualizeEditor visualizePlan={visualizePlan} />
			{cyElements && (
				<Box mt="32px">
					<Heading as="h1" size="lg" mb="16px">
						Plan
					</Heading>
					<Box>Hover over the nodes to obtain more details.</Box>
					<Box border="1px solid #d1d1d1">
						<ColoredExecutionPlanner
							mode="view"
							allowPanning={true}
							allowZooming={true}
							suggestedExecutionPlan={cyElements}
						/>
					</Box>
				</Box>
			)}
		</Box>
	);
};

export default VisualizerPage;
