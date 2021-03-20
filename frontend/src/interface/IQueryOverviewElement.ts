export interface IQueryOverviewElement {
	_id: string;
	status: "done" | "timeout" | "pending";
	result_count: number;
	t_delta: number;
	requests: number;
	t_start: string;
	query: string;
	query_name?: string;
}
