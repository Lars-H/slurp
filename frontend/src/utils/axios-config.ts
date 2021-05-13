import axios from "axios";
import { IQueryOverviewElement } from "interface/IQueryOverviewElement";
import { ITaskPageDataResponse } from "interface/ITaskPageDataResponse";

interface IGetExecutionPlan {
	query: string;
	sources: string;
	optimizer: string;
}

interface IGetExecutionsForIdenticalQuery {
	hash: string;
	plan: string;
}

interface IExecuteQueryAsync {
	// TODO
	plan: any;
	sources: string[];
	query: string;
	query_name: string | null;
}

const buildBaseURL = () => {
	if (process.env.NODE_ENV === "production") {
		console.log("PRODUCTION");
		if (process.env.REACT_APP_BASE_URL && process.env.REACT_APP_API_PROXY) {
			return process.env.REACT_APP_BASE_URL.concat(process.env.REACT_APP_API_PROXY);
		} else {
			console.log("ERROR");
			return "http://localhost:5000/";
		}
	} else {
		return "http://localhost:5000/";
		// .concat(process.env.REACT_APP_API_PROXY)
	}
};

export const API = axios.create({
	baseURL: buildBaseURL(),
});

export default {
	getExecutionPlan: (data: IGetExecutionPlan) =>
		API.get("/plan", {
			params: data,
		}),
	executeQueryAsync: (params: IExecuteQueryAsync) => API.post("/plan", params),
	getResult: (resultId: string) => API.get<ITaskPageDataResponse>(`result/${resultId}`),
	getRequestList: () => API.get<IQueryOverviewElement[]>("/result"),
	getFilteredList: (name: string) => API.get(`result/filter/${name}`),
	getExecutionsForIdenticalQuery: (data: IGetExecutionsForIdenticalQuery) =>
		API.get("/executions/hash", { params: data }),
};
