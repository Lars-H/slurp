import React, { Component } from "react";
import api from "utils/axios-config";
import { Button, VStack, Heading, HStack, IconButton, Input } from "@chakra-ui/react";
import QueryBox from "components/QueryBox/QueryBox";
import { timeConverter } from "../../utils/utils";
import { RepeatIcon, AddIcon } from "@chakra-ui/icons";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { IAlertProps } from "components/HoCs/withAlert";
import { IQueryOverviewElement } from "interface/IQueryOverviewElement";

interface IMainPageState {
	requestList: IQueryOverviewElement[];
}

class MainPage extends Component<RouteComponentProps & IAlertProps, IMainPageState> {
	state: IMainPageState = { requestList: [] };

	componentDidMount = () => {
		this.getRequestList();
	};

	getRequestList = async () => {
		let response;
		try {
			response = await api.getRequestList();
		} catch (err) {
			if (!err.response || !err.response.data) {
				return;
			}
			const errData = err.response.data;
			if (errData && errData.msg && errData.title) {
				this.props.setAlert({
					...errData,
					status: "error",
				});
			}
			return;
		}
		this.setState({ requestList: response.data });
	};

	getFilteredList = async (queryName: string) => {
		try {
			const response = await api.getFilteredList(queryName);
			this.setState({ requestList: response.data });
			console.log(response.data);
		} catch (error) {
			console.error(error);
		}
	};

	handleFilterOnChange = () => {
		const queryName = (document.querySelector("#queryname") as HTMLInputElement).value;
		queryName ? this.getFilteredList(queryName) : this.getRequestList();
	};

	forwardToCreateQueryPage = () => {
		this.props.history.push("/new");
	};

	render() {
		return (
			<>
				<VStack align="flex-start">
					<HStack position="relative" w="100%">
						<Heading m="3">Queries</Heading>
						<IconButton
							onClick={this.getRequestList}
							aria-label="refresh query list"
							icon={<RepeatIcon />}
							mr="3"
						/>
						<Input
							id="queryname"
							width="250px"
							type="text"
							placeholder="Filter by name"
							onChange={this.handleFilterOnChange}
						/>
						<Button
							colorScheme="red"
							position="absolute"
							right="0"
							left="auto"
							onClick={this.forwardToCreateQueryPage}
						>
							<AddIcon mr="2" /> New Query
						</Button>
					</HStack>
					{this.state.requestList.map((el: IQueryOverviewElement) => {
						return (
							<QueryBox
								key={el._id}
								id={el._id}
								status={el.status}
								resultCount={el.result_count}
								tDelta={el.t_delta}
								requests={el.requests}
								tStart={timeConverter(el.t_start)}
								query={el.query}
								queryName={el.query_name}
							/>
						);
					})}
				</VStack>
			</>
		);
	}
}

export default withRouter(MainPage);
