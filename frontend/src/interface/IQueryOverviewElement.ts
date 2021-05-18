import {TaskStatus} from "./ITaskPageDataResponse";

export interface IQueryOverviewElement {
	_id: string;
	status: TaskStatus;
	result_count: number;
	t_delta: number;
	requests: number;
	t_start: string;
	query: string;
	query_name?: string;
}
