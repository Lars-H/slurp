import React, { Component } from "react";
import api from "utils/axios-config";
import BinaryTree from "utils/DataStructures/binaryTree";

import { Stack } from "@chakra-ui/react";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";
import QueryEditor from "components/QueryEditor/QueryEditor";

import withAlert, { IAlertProps } from "components/HoCs/withAlert";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { logger } from "utils/logger";
import planOne from "../../examplePlans/Aktuell.json";

interface ICreateQueryPageState {
	sources: any;
	suggestedExecutionPlan: null;
	executionPlanSubmitted: boolean;
	query: string;
	queryName?: string;
	querySubmitted: boolean;
	optimizer: string;
}

class CreateQueryPage extends Component<IAlertProps & RouteComponentProps, ICreateQueryPageState> {
	state: ICreateQueryPageState = {
		sources: [],
		suggestedExecutionPlan: null,
		executionPlanSubmitted: false,
		query: "",
		querySubmitted: false,
		optimizer: "",
	};

	componentDidMount() {
		const tree = new BinaryTree();
		tree.buildTreeFromExecutionPlan(planOne, "Temp");
		const treeElements = tree.getElements();
	}

	submitQuery = async (
		query: string,
		sources: any,
		optimizerName: string,
		queryName?: string
	) => {
		logger("Retrieving Execution Plan");
		this.setState({
			query: query,
			sources: sources,
			queryName: queryName,
			optimizer: optimizerName,
		});

		// GET request parameters are base64 encoded due to the special characters in the query
		const payload = {
			query: btoa(query),
			sources: btoa(sources),
			optimizer: btoa(optimizerName),
		};

		logger("Query an Backend gesendet: ");
		logger(query);
		logger(optimizerName);
		logger(`Sources: ${sources}`);
		logger("---");

		let response;
		try {
			response = await api.getExecutionPlan(payload);
		} catch (err) {
			const errData = err.response.data;
			if (errData && errData.msg && errData.title) {
				this.props.setAlert({
					...errData,
					status: "error",
				});
				this.setState({
					query: "",
				});
			}
			return;
		}

		// Only store submitted query after a successfull request. If the request was successfull,
		// the user cannot submit the query again until he pushed on the reset button
		// this.setState({ query: query });
		this.props.setAlert(null);

		logger("Recommended Execution Plan vom Server erhalten:");
		logger(response.data);

		const tree = new BinaryTree();
		tree.buildTreeFromExecutionPlan(response.data, query);
		const treeElements = tree.getElements();

		logger("Execution Plan aus folgenden Elementen gebaut:");
		logger(treeElements);

		this.setState({
			suggestedExecutionPlan: treeElements,
		});
	};

	executePlan = async (executionPlan) => {
		this.setState({
			executionPlanSubmitted: true,
		});
		const payload = {
			plan: executionPlan,
			sources: this.state.sources,
			query: this.state.query,
			// Backend currently cant pross query if it receives undefined as queryName !?
			query_name: this.state.queryName ? this.state.queryName : null,
		};

		logger(payload);

		logger("Executing Plan:");
		logger(executionPlan);

		try {
			const response = await api.executeQueryAsync(payload);
			logger(response.data);
			this.props.history.push(`/task/${response.data["task_id"]}`);
		} catch (err) {
			logger(err);
		}
		this.setState({
			executionPlanSubmitted: false,
		});
	};

	resetSubmition = () => {
		this.setState({
			query: "",
			suggestedExecutionPlan: null,
			sources: [],
			queryName: "",
			querySubmitted: false,
		});
	};

	render() {
		return (
			<>
				<Stack shouldWrapChildren spacing="32px">
					<QueryEditor
						mode="edit"
						querySubmitted={this.state.query ? true : false}
						resetSubmition={this.resetSubmition}
						submitQuery={this.submitQuery}
					/>

					{this.state.suggestedExecutionPlan && (
						<ColoredExecutionPlanner
							mode="edit"
							suggestedExecutionPlan={this.state.suggestedExecutionPlan}
							executePlan={this.executePlan}
							executionPlanSubmitted={this.state.executionPlanSubmitted}
						/>
					)}
				</Stack>
			</>
		);
	}
}

export default withRouter(withAlert(CreateQueryPage));
