import React, { Component, createRef } from "react";

import cxtmenu from "cytoscape-cxtmenu";
import "../../utils/extendedCytoscape";

import { deepCompare } from "utils/utils";

import { InvalidExecutionPlanError } from "./InvalidExecutionPlanError";

import { CytoscapeService } from "./CytoscapeService";

import withAlert, { IAlertProps } from "components/HoCs/withAlert";

import { Button, Stack, HStack, Box, Heading } from "@chakra-ui/react";
import cyStyleDark from "./cy-style-dark";
import cyStyleLight from "./cy-style-light";

import HelpModal from "./HelpModal";
import CytoscapeComponent from "react-cytoscapejs";

const cytoscapeService = new CytoscapeService();
const MODE_EDIT = "edit";
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const MODE_VIEW = "view";

interface IExecutionPlannerProps {
	mode: "edit" | "view";
	executionPlanSubmitted: boolean;
	dark: boolean;
	suggestedExecutionPlan: cytoscape.ElementDefinition[];
	executePlan: any;

	allowPanning?: boolean;
	allowZooming?: boolean;
}

class ExecutionPlanner extends Component<IAlertProps & IExecutionPlannerProps> {
	cy?: cytoscape.Core;
	cyRef: React.RefObject<HTMLDivElement>;

	constructor(props: IAlertProps & IExecutionPlannerProps) {
		super(props);
		this.cyRef = createRef();
	}
	state = {
		cyContainerStyle: {
			// width: "100%",
			height: "100px",
		},
	};

	adjustHeightCallback = (height: string) => {
		this.setState({
			cyContainerStyle: {
				...this.state.cyContainerStyle,
				height: height,
			},
		});
	};

	cyReady = (cy: cytoscape.Core) => {
		let cyMaxWidth: number | undefined = undefined;
		if (this.props.mode === MODE_EDIT && this.cyRef.current) {
			cyMaxWidth = this.cyRef.current.offsetWidth;
		}
		cytoscapeService.registerCytoscapeInstance(
			cy,
			this.props.mode,
			this.props.suggestedExecutionPlan,
			this.adjustHeightCallback,
			cyMaxWidth
		);
	};

	componentWillUnmount() {
		// Required for hot module reload
		if (this.cy) {
			Object.getPrototypeOf(this.cy)[cxtmenu] = null;
		}
		cytoscapeService.unregisterService();

		if (this.props.mode === MODE_EDIT) {
			window.removeEventListener("resize", this.updateCyMaxWidth);
		}
	}

	componentDidUpdate = (prevProps: IAlertProps & IExecutionPlannerProps) => {
		// Updates Layout if initial elements changes
		// e.g. happens if backend provides new parameters like join cardinality after finished execution
		if (
			!deepCompare(prevProps.suggestedExecutionPlan, this.props.suggestedExecutionPlan) &&
			prevProps.suggestedExecutionPlan
		) {
			cytoscapeService.updateCyContainer();
		}
	};

	executePlan = () => {
		let executionPlanJSON;
		try {
			executionPlanJSON = cytoscapeService.getExecutionPlan();
			this.props.setAlert(null);
			this.props.executePlan(executionPlanJSON);
		} catch (err) {
			if (err instanceof InvalidExecutionPlanError) {
				if (err.title && err.msg) {
					this.props.setAlert({ ...err, status: "error" });
				}
			}
		}
	};

	componentDidMount() {
		if (this.props.mode === MODE_EDIT) {
			window.addEventListener("resize", this.updateCyMaxWidth);
		}
	}
	updateCyMaxWidth = () => {
		if (this.cyRef && this.cyRef.current) {
			cytoscapeService.updateMaxWidth(this.cyRef.current.offsetWidth);
		}
	};

	render() {
		const allowZooming = this.props.allowZooming !== undefined ? true : false;
		const allowPanning = this.props.allowPanning !== undefined ? true : false;
		return (
			<>
				{this.props.suggestedExecutionPlan && (
					<>
						{this.props.mode === MODE_EDIT ? (
							<div>
								<HStack mb="16px">
									<Heading as="h1" size="lg">
										Execution Plan
									</Heading>
									<HelpModal />
								</HStack>
								<div ref={this.cyRef} />
								<Box border="1px solid #d1d1d1">
									<CytoscapeComponent
										elements={this.props.suggestedExecutionPlan}
										style={this.state.cyContainerStyle}
										stylesheet={this.props.dark ? cyStyleDark : cyStyleLight}
										userZoomingEnabled={false}
										userPanningEnabled={false}
										cy={this.cyReady}
									/>
								</Box>
								<Button
									mt="16px"
									onClick={this.executePlan}
									isLoading={this.props.executionPlanSubmitted}
									disabled={this.props.executionPlanSubmitted}
								>
									Execute
								</Button>
							</div>
						) : (
							<Stack shouldWrapChildren spacing="16px">
								<CytoscapeComponent
									elements={this.props.suggestedExecutionPlan}
									style={this.state.cyContainerStyle}
									stylesheet={this.props.dark ? cyStyleDark : cyStyleLight}
									userZoomingEnabled={allowZooming}
									userPanningEnabled={allowPanning}
									cy={this.cyReady}
								/>
							</Stack>
						)}
					</>
				)}
			</>
		);
	}
}

// ExecutionPlanner.propTypes = {
//   mode: PropTypes.oneOf([MODE_EDIT, MODE_VIEW]).isRequired,
// };

export default withAlert(ExecutionPlanner);
