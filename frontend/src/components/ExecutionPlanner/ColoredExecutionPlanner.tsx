import React from "react";
import { useColorModeValue } from "@chakra-ui/react";
import ExecutionPlanner from "./ExecutionPlanner";

const ColoredExecutionPlanner = (props) => {
	const dark = useColorModeValue(false, true);
	return <ExecutionPlanner {...props} dark={dark} />;
};

export default ColoredExecutionPlanner;
