export type ProcessingQueryStatusTypes = "pending" | "queue" | "done" | "timeout" | "failed";

export interface ITaskPageDataResponse {
	_id: string;
	plan: any;
	query: string;
	query_hash: string;
	plan_hash: string;
	query_name: string;
	requests: number;
	result_count: number;
	sources: string[];
	sparql_results: any;
	status: ProcessingQueryStatusTypes;
	t_delta: number;
	t_end?: number;
	t_start: number;
}
