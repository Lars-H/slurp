import {Component} from "react";
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
} from "@chakra-ui/react";
import BinaryTree from "utils/DataStructures/binaryTree";
import Alert from "components/Alert/Alert";
import withAlert, {IAlertProps} from "components/HoCs/withAlert";
import {deepCompare} from "utils/utils";
import {RouteComponentProps} from "react-router-dom";
import {ITaskPageDataResponse, TaskStatus} from "interface/ITaskPageDataResponse";
import TaskOverview from "components/TaskOverview/TaskOverview";
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
	comparandExecutionPlan?: ITaskPageDataResponse;
	syncedExtendedAccordionItems: ExpandedIndex;
}

const RETRIEVE_RESULTS_INTERVAL = 5000;

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
					this.setState({task: response.data});

					setTimeout(() => {
						this.getTaskInfo();
					}, RETRIEVE_RESULTS_INTERVAL);

					this.fetchDifferentExececutionPlansForIdenticalQuery();

					this.setState({
						fetchingResults: false,
					});
				})
				.catch((err) => {
					this.setState({
						fetchingResults: false,
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
			this.setState({executionsForSameQuery: response.data});
		} catch (err) {
			console.log(err);
		}
	};

	transformExecutionPlanForCy = () => {
		const task = this.state.task;
		if (task) {
			const tree = new BinaryTree();
			tree.buildTreeFromExecutionPlan(task.plan, task.query);
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
			comparandExecutionPlan: undefined,
		});
	};

	compareWithExecutionPlan = (id: string) => {
		if (!this.state.executionsForSameQuery) {
			return;
		}
		const comparandIndex = this.state.executionsForSameQuery.findIndex((el) => el._id === id);

		if (comparandIndex !== -1) {
			this.setState({
				comparandExecutionPlan: this.state.executionsForSameQuery[comparandIndex],
				syncedExtendedAccordionItems: [],
			});
		}
	};

	updateExtendedItems = (extendedItems: ExpandedIndex) => {
		this.setState({
			syncedExtendedAccordionItems: extendedItems,
		});
	};

	render() {
		const splitViewActive = typeof this.state.comparandExecutionPlan !== "undefined";
		return (
			<>
				{this.state.task ? (
					<Stack>
						{this.createAlertInfo()}

						{this.state.executionsForSameQuery.length > 0 && (
							<Box mt={4}>
								{this.state.isComparingExecutionPlans ? (
									<>
										<Flex>
											<Select
												placeholder="Select option"
												onChange={(evt) =>
													this.compareWithExecutionPlan(
														evt.target.value
													)
												}
											>
												{this.state.executionsForSameQuery.map((el) => {
													return (
														<Tooltip key={el._id} label="lol">
															<option value={el._id}>
																{el._id}
															</option>
														</Tooltip>
													);
												})}
											</Select>
											<Button
												ml={2}
												onClick={this.toggleCompareExecutions}
											>
												Hide
												</Button>
										</Flex>
									</>
								) : (
									<Flex>
										<Text color="gray.400" fontSize="18px">
											There are different execution plans for the same
											query
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

						<Flex>
							{this.state.comparandExecutionPlan ? (
								<TwoTasksComparison
									first={this.state.task}
									second={this.state.comparandExecutionPlan}/>
							) : (
								<TaskOverview
									{...this.state.task}
								/>
							)}
						</Flex>
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
