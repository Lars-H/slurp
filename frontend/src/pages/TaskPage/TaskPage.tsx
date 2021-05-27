import React, { Component } from "react";
import api from "utils/axios-config";
import {
	Stack,
	Spinner,
	Box,
	Center,
	Text,
	Flex,
	Select,
	Button,
	Tooltip,
	ExpandedIndex,
	Heading,
} from "@chakra-ui/react";
import BinaryTree from "utils/DataStructures/binaryTree";
import Alert from "components/Alert/Alert";
import withAlert, { IAlertProps } from "components/HoCs/withAlert";
import { deepCompare } from "utils/utils";
import { RouteComponentProps } from "react-router-dom";
import { ITaskPageDataResponse, TaskStatus } from "interface/ITaskPageDataResponse";
import TaskOverview, { OverviewElements } from "components/TaskOverview/TaskOverview";
import TwoTasksComparison from "components/TaskOverview/TwoTasksComparison";

interface IMatchParams {
	taskId: string;
}

type IMatchProps = RouteComponentProps<IMatchParams>;

interface ITaskPageState {
	task?: ITaskPageDataResponse;
	taskId: string;
	planCy: any;
	fetchingResults: boolean;

	executionsForSameQuery: ITaskPageDataResponse[];
	isComparingExecutionPlans: boolean;
	syncedExtendedAccordionItems: ExpandedIndex;
}

const RETRIEVE_RESULTS_INTERVAL = 5000;

const hasTaskFinished = (status: TaskStatus) => {
	return [TaskStatus.done, TaskStatus.timeout, TaskStatus.failed].includes(status);
};

class TaskPage extends Component<IAlertProps & IMatchProps, ITaskPageState> {
	state: ITaskPageState = {
		taskId: this.props.match.params.taskId,
		planCy: null,
		fetchingResults: false,
		executionsForSameQuery: [],
		isComparingExecutionPlans: false,
		syncedExtendedAccordionItems: [0, 3],
	};

	getTaskInfo = async () => {
		if (
			!this.state.task ||
			this.state.task.status === "pending" ||
			this.state.task.status === "queue"
		) {
			console.log(`Fetching results from task ${this.state.taskId}`);

			this.setState({
				fetchingResults: true,
			});

			api.getResult(this.state.taskId)
				.then((response) => {
					this.setState({ task: response.data });

					console.log(response.data.sparql_results)

					setTimeout(() => {
						this.getTaskInfo();
					}, RETRIEVE_RESULTS_INTERVAL);

					this.fetchDifferentExececutionPlansForIdenticalQuery();
				})
				.catch((err) => {
					// Generate Alert Message
					const errData = err.response.data;
					if (errData && errData.msg && errData.title) {
						this.props.setAlert({
							...errData,
							status: "error",
						});
					}
				});
		}

		this.setState({
			fetchingResults: false,
		});
	};

	fetchDifferentExececutionPlansForIdenticalQuery = async () => {
		if (!this.state.task) {
			return;
		}

		const payload = {
			query_hash: btoa(this.state.task.query_hash),
			plan_hash: btoa(this.state.task.plan_hash),
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
		const task = this.state.task;
		if (!task) {
			throw new Error("No task available");
		}

		const tree = new BinaryTree();
		tree.buildTreeFromExecutionPlan(task.plan, task.query);
		const treeElements = tree.getElements();
		this.setState({
			planCy: treeElements,
		});
	};

	componentDidMount = async () => {
		await this.getTaskInfo();
	};

	componentDidUpdate = (_: any, prevState: ITaskPageState) => {
		const taskCurrentState = this.state.task;
		const taskPrevState = prevState.task;
		if (!taskCurrentState || !taskPrevState) {
			return;
		}

		if (!deepCompare(taskCurrentState.plan, taskPrevState.plan)) {
			this.transformExecutionPlanForCy();
		}
	};

	createAlertInfo = () => {
		if (!this.state.task || !this.state.task.sparql_results) {
			return;
		}

		const status = this.state.task.status;

		if (status === TaskStatus.pending) {
			return (
				<Alert
					title="Query is currently processed"
					description="The results are refreshed periodically"
					status="info"
				/>
			);
		}
		if (status === TaskStatus.queue) {
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
		this.setState({
			isComparingExecutionPlans: !this.state.isComparingExecutionPlans,
		});
	};

	render() {
		return (
			<>
				{this.state.task ? (
					<Stack>
						{this.createAlertInfo()}

						{this.state.isComparingExecutionPlans ? (
							<TwoTasksComparison
								first={this.state.task}
								others={this.state.executionsForSameQuery}
								leaveComparison={this.toggleCompareExecutions}
							/>
						) : (
							<>
								<Box pl="16px" mb="16px">
									<Flex alignContent="center" mb="16px" justifyContent='space-between'>
										<Heading as="h1" size="md" marginY="auto">
											Task {this.state.task._id}
										</Heading>
										{this.state.executionsForSameQuery.length > 0 &&
											!this.state.isComparingExecutionPlans &&
											hasTaskFinished(this.state.task.status) && (
												<Box>
													<Flex>
														<Button
															display="inline"
															colorScheme="gray"
															onClick={this.toggleCompareExecutions}
														>
															Compare with other plans
														</Button>
													</Flex>
												</Box>
											)}
									</Flex>
									{this.state.task.query_name && (
										<Heading mr="1" size="sm">
											Name: {this.state.task.query_name}
										</Heading>
									)}
								</Box>

								<TaskOverview {...this.state.task} splitView={false} />
							</>
						)}
					</Stack>
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
