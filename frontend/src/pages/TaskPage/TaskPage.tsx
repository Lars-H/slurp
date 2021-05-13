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
	Flex,
	Heading,
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
import { logger } from "utils/logger";

interface IMatchParams {
	taskId: string;
}

type IMatchProps = RouteComponentProps<IMatchParams>;

interface ITaskPageState extends Partial<ITaskPageDataResponse> {
	taskId: string;
	planCy: any;
	fetchingResults: boolean;
	executionsForSameQuery?: Partial<ITaskPageDataResponse>[];
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
		requests: 0,
		status: undefined,
		fetchingResults: false,
		executionsForSameQuery: undefined,
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
		if (!this.state.query_hash) {
			return;
		}

		const payload = {
			hash: btoa(this.state.query_hash.toString()),
			plan: btoa(this.state.plan),
		};

		let response;
		try {
			response = await api.getExecutionsForIdenticalQuery(payload);

			console.log("Before filter");
			console.log(response.data);
			const data = response.data.filter((el) => !deepCompare(el.plan, this.state.plan));
			console.log("After filter");
			console.log(data);

			this.setState({ executionsForSameQuery: data });
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

	render() {
		return (
			<>
				{this.state.query ? (
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
						<Accordion defaultIndex={[0, 3]} allowMultiple>
							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Information
									</Box>
									<AccordionIcon />
								</AccordionButton>

								<AccordionPanel pb={4}>
									<Stack shouldWrapChildren spacing="32px">
										<MetaBadges
											status={this.state.status}
											resultCount={this.state.result_count}
											requests={this.state.requests}
											tDelta={this.state.t_delta}
											showRequestHint={true}
											tStart={timeConverter(this.state.t_start)}
											tEnd={
												this.state.t_end && timeConverter(this.state.t_end)
											}
										/>
										<Flex wrap="wrap" mt="-5">
											{this.state.sources &&
												this.state.sources.map((el) => {
													return <Badge key={el}>{el}</Badge>;
												})}
										</Flex>
									</Stack>
								</AccordionPanel>
							</AccordionItem>

							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Query
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<QueryEditor mode="view" query={this.state.query} />
								</AccordionPanel>
							</AccordionItem>

							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Execution Plan
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<ColoredExecutionPlanner
										mode="view"
										suggestedExecutionPlan={this.state.planCy}
									/>
								</AccordionPanel>
							</AccordionItem>

							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Results
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<ResultTable
										results={this.state.sparql_results}
										status={this.state.status}
									/>
								</AccordionPanel>
							</AccordionItem>
						</Accordion>
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
