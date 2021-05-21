export enum TaskStatus {
	pending = 'pending',
	queue = 'queue',
	done = 'done',
	timeout = 'timeout',
	failed = 'failed',
}

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
	status: TaskStatus;
	t_start: number;
	t_end?: number;
	t_delta: number;
}
