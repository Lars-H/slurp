export type JoinType = "SHJ" | "NLJ" | "FJoin";

export interface InnerNode {
	type: JoinType; // currently wrong in backend
	joinOperator: JoinType;
	left?: Node;
	right?: Node;
	estimated_tuples?: number;
	produced_tuples?: number;
	[key: string]: any;
	test: string;
}

export type Node = InnerNode | Leaf;

export interface Leaf {
	type: "Leaf";
	cardinality?: number;
	tpf: string;
	[key: string]: any;
	test: string;
}

export type ExecutionPlan = Node;
