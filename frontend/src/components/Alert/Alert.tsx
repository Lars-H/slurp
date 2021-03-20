import React from "react";
import PropTypes from "prop-types";
import {
	Alert as ChakraAlert,
	AlertIcon,
	Box,
	AlertTitle,
	AlertDescription,
} from "@chakra-ui/react";

export type AlertStatus = "error" | "success" | "warning" | "info";

interface IAlertProps {
	title: string;
	description: string;
	status: AlertStatus;
}

const Alert: React.FC<IAlertProps> = (props: IAlertProps) => {
	const { title, description, status = "error" } = props;

	return (
		<ChakraAlert mb="4" status={status}>
			<AlertIcon />
			<Box flex="1">
				<AlertTitle>{title}</AlertTitle>
				<AlertDescription display="block">{description}</AlertDescription>
			</Box>
		</ChakraAlert>
	);
};

Alert.propTypes = {
	title: PropTypes.string.isRequired,
	description: PropTypes.string.isRequired,
	status: PropTypes.oneOf<AlertStatus>(["error", "success", "warning", "info"]).isRequired,
};

export default Alert;
