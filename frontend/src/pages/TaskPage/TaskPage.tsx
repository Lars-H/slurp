import React, { Component } from "react";
import api from "utils/axios-config";
import {
	Stack,
	Spinner,
	Badge,
	AccordionItem,
	Accordion,
	AccordionButton,
	Box,
	AccordionIcon,
	AccordionPanel,
	Center,
	Text,
	Flex,
	Heading,
	Select,
	Button,
	Tooltip,
} from "@chakra-ui/react";
import BinaryTree from "utils/DataStructures/binaryTree";
import { timeConverter } from "../../utils/utils";
import ColoredExecutionPlanner from "components/ExecutionPlanner/ColoredExecutionPlanner";
import ResultTable from "components/ResultTable/ResultTable";
import QueryEditor from "components/QueryEditor/QueryEditor";
import Alert from "components/Alert/Alert";
import withAlert, { IAlertProps } from "components/HoCs/withAlert";
import MetaBadges from "components/MetaBadges/MetaBadges";
import { deepCompare } from "utils/utils";
import { RouteComponentProps } from "react-router-dom";
import { ITaskPageDataResponse } from "interface/ITaskPageDataResponse";
import TaskOverview from "components/TaskOverview/TaskOverview";

interface IMatchParams {
	taskId: string;
}

type IMatchProps = RouteComponentProps<IMatchParams>;

interface ITaskPageState extends Partial<ITaskPageDataResponse> {
	taskId: string;
	planCy: any;
	fetchingResults: boolean;

	executionsForSameQuery: Partial<ITaskPageDataResponse>[];
	isComparingExecutionPlans: boolean;
	comparandExecutionPlan?: Partial<ITaskPageDataResponse>;
}

const RETRIEVE_RESULTS_INTERVAL = 5000;

class TaskPage extends Component<IAlertProps & IMatchProps, ITaskPageState> {
	state: ITaskPageState = {
		taskId: this.props.match.params.taskId,
		plan: null,
		planCy: null,
		sources: [],
		sparql_results: null,
		query: "",
		query_name: "",
		plan_hash: "",
		requests: 0,
		status: undefined,
		fetchingResults: false,
		executionsForSameQuery: [],
		isComparingExecutionPlans: false,
	};

	getTaskInfo = async () => {
		if (
			!this.state.status ||
			this.state.status === "pending" ||
			this.state.status === "queue"
		) {
			console.log(`Fetching results from task ${this.state.taskId}`);

			this.setState({
				fetchingResults: true,
			});

			api.getResult(this.state.taskId)
				.then((response) => {
					this.setState(response.data);
					console.log(response.data);
					this.setState({ ...response.data });

					setTimeout(() => {
						this.getTaskInfo();
					}, RETRIEVE_RESULTS_INTERVAL);

					this.fetchDifferentExececutionPlansForIdenticalQuery();

					this.setState({
						fetchingResults: false,
					});
				})
				.catch((err) => {
					// Stop fetching
					this.setState({
						fetchingResults: false,
						status: "failed",
					});

					// Generate Alert Message
					const errData = err.response.data;
					if (errData && errData.msg && errData.title) {
						this.props.setAlert({
							...errData,
							status: "error",
						});
					}
				});
		} else {
			this.setState({
				fetchingResults: false,
			});
		}
	};

	fetchDifferentExececutionPlansForIdenticalQuery = async () => {
		console.log(this.state.query_hash);
		if (!this.state.query_hash || !this.state.plan_hash) {
			return;
		}

		const payload = {
			query_hash: btoa(this.state.query_hash),
			plan_hash: btoa(this.state.plan_hash),
		};

		let response;
		try {
			response = await api.getExecutionsForIdenticalQuery(payload);
			this.setState({ executionsForSameQuery: response.data });
		} catch (err) {
			console.log(err);
		}
	};

	transformExecutionPlanForCy = () => {
		if (this.state.plan && this.state.query) {
			const tree = new BinaryTree();
			tree.buildTreeFromExecutionPlan(this.state.plan, this.state.query);
			const treeElements = tree.getElements();

			this.setState({
				planCy: treeElements,
			});
		} else {
			console.log("Error. Execution plan or Query not available");
		}
	};

	componentDidMount = async () => {
		await this.getTaskInfo();
	};

	componentDidUpdate = (_: any, prevState: ITaskPageState) => {
		if (!deepCompare(prevState.plan, this.state.plan)) {
			this.transformExecutionPlanForCy();
		}
	};

	createAlertInfo = () => {
		if (this.state.sparql_results && this.state.status === "pending") {
			return (
				<Alert
					title="Query is currently processed"
					description="The results are refreshed periodically"
					status="info"
				/>
			);
		}
		if (this.state.sparql_results && this.state.status === "queue") {
			return (
				<Alert
					title="Query is currently waiting in queue"
					description="This page is refreshed periodically"
					status="info"
				/>
			);
		}
		return null;
	};

	toggleCompareExecutions = () => {
		this.setState({ isComparingExecutionPlans: !this.state.isComparingExecutionPlans });
	};

	compareWithExecutionPlan = (id: string) => {
		if (!this.state.executionsForSameQuery) {
			return;
		}
		const comparandIndex = this.state.executionsForSameQuery.findIndex((el) => el._id === id);

		if (comparandIndex !== -1) {
			this.setState({
				comparandExecutionPlan: this.state.executionsForSameQuery[comparandIndex],
			});
		}
	};

	render() {
		return (
			<>
				{this.state.query ? (
					<>
						<Stack>
							<Flex wrap="wrap" mb="5" align="center" justifyContent="space-between">
								<Heading as="h1" size="lg">
									Task {this.state.taskId}
								</Heading>

								{this.state.query_name && (
									<Heading mr="1" size="md">
										Name: {this.state.query_name}
									</Heading>
								)}
							</Flex>
							{this.createAlertInfo()}

							<TaskOverview {...this.state} />
							{/* <TaskOverview {...this.state} _id="wegweg" /> */}
						</Stack>

						{this.state.executionsForSameQuery.length > 0 && (
							<Box mt={4}>
								{this.state.isComparingExecutionPlans ? (
									<>
										<Flex>
											<Select
												placeholder="Select option"
												onChange={(evt) =>
													this.compareWithExecutionPlan(evt.target.value)
												}
											>
												{this.state.executionsForSameQuery.map((el) => {
													return (
														<Tooltip key={el._id} label="lol">
															<option value={el._id}>{el._id}</option>
														</Tooltip>
													);
												})}
											</Select>
											<Button ml={2} onClick={this.toggleCompareExecutions}>
												Hide
											</Button>
										</Flex>
										{this.state.comparandExecutionPlan && (
											<TaskOverview {...this.state.comparandExecutionPlan} />
										)}
									</>
								) : (
									<Flex>
										<Text color="gray.400" fontSize="18px">
											There are different execution plans for the same query
										</Text>
										<Button
											display="inline"
											onClick={this.toggleCompareExecutions}
										>
											Compare
										</Button>
									</Flex>
								)}
							</Box>
						)}
					</>
				) : (
					<Center>
						<Spinner mt="10" size="xl" />
					</Center>
				)}
			</>
		);
	}
}

export default withAlert(TaskPage);
