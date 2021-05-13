import { ExecutionPlan } from "interface/ExecutionPlan";
import { v4 as uuidv4 } from "uuid";
import { deleteEmptyFields } from "../utils";

type PrefixValuePair = Array<string>;

/*
This class has two use cases:
  1) Transform a JSON execution plan in an array 
  containing elements for the library cytoscape. https://js.cytoscape.org/#getting-started/specifying-basic-options

  2) Transform cytoscape elements array into a JSON execution plan.
*/
class BinaryTree {
	root?: any; //TODO REPLACE LATER
	tpfToLineNumberInEditor = {};
	elements: any;

	// Creates a tree based on a JSON execution plan and the entered query
	buildTreeFromExecutionPlan = (data: any, query: string) => {
		this.initializeTpfToLabel(query);
		this.createTree(data);
	};

	// Recursive function to create a binary tree

	/**  
		Recursive function to create a binary tree data structure.
		@param tree The execution plan which is built as a binary tree.
	*/
	createTree = (tree: ExecutionPlan) => {
		if (!tree.type) {
			return;
		}

		const nodeType = tree.type === "Leaf" ? "Leaf" : "InnerNode";

		let node: Leaf | InnerNode;
		if (nodeType === "Leaf") {
			const { tpf, ...otherProperties } = tree;
			// The label of each Triple Pattern is represented by its line number in the Query Editor.
			const label = this.tpfToLineNumberInEditor[tree.tpf];
			node = new Leaf(label, tree.tpf, otherProperties);
		} else {
			// Inner Node
			const { type, ...otherProperties } = tree;
			const joinOperator = type;
			node = new InnerNode(joinOperator, otherProperties);
		}

		if (!this.root) {
			this.root = node;
		}

		if (tree.left) {
			node.left = this.createTree(tree.left);
		}
		if (tree.right) {
			node.right = this.createTree(tree.right);
		}
		return node;
	};

	// Represent the tree as an array which is used for the cytoscape library as input.
	getElements = () => {
		this.elements = [];
		this.generateElements(this.root);
		return this.elements;
	};

	generateElements = (node: InnerNode | Leaf | undefined) => {
		if (!node) {
			return;
		}

		const cyNode = {
			data: node.getCyData(),
			group: "nodes",
			classes: "center-center",
		};
		this.elements.push(cyNode);

		if (node.type !== "InnerNode") {
			return;
		}

		// Create edges if the observed node has children
		if (node.left) {
			const leftEdge = {
				data: {
					source: cyNode.data.id,
					target: node.left.id,
				},
				group: "edges",
			};
			this.elements.push(leftEdge);
		}
		if (node.right) {
			const rightEdge = {
				data: {
					source: cyNode.data.id,
					target: node.right.id,
				},
				group: "edges",
			};
			this.elements.push(rightEdge);
		}

		this.generateElements(node.left);
		this.generateElements(node.right);
	};

	// Each Leaf in the Cytoscape graph represents a Triple Pattern.
	// The Leaves are labeled with the line numbers of the Triple Patterns from the Query Editor.
	// Each line of the query is stores a key in a dictionary and the value represents its line number.
	// ASSUMPTION: EXACTLY ONE tripple pattern per row
	initializeTpfToLabel = (query: string) => {
		const lines = query.split("\n").map((s) => s.trim());

		const prefixes: Array<PrefixValuePair> = [];

		for (const i of lines) {
			const match = i.match(/(?<=PREFIX).*:.*>/i);
			if (match) {
				// TODO Due to previous formatting we have the assumption that at most one Prefix can be defined per line
				const result = match[0].trim();
				const indexColon = result.indexOf(":");
				const prefixValueSplit = [
					result.slice(0, indexColon),
					result.slice(indexColon + 1),
				];
				prefixes.push(prefixValueSplit);
			}
		}

		for (let i = 0; i < lines.length; i++) {
			const formattedLine = this.replacePrefixes(prefixes, lines[i]);

			// +1 because lines in Query Editor start from number 1
			this.tpfToLineNumberInEditor[formattedLine] = i + 1;
		}
	};

	replacePrefixes = (prefixes: Array<PrefixValuePair>, line: string) => {
		// Dont consider the lines where the prefixes have been defined
		if (line.includes("PREFIX")) {
			return line;
		}

		let formattedLine = line;
		for (const prefixValuePair of prefixes) {
			const prefix = prefixValuePair[0];
			const value = prefixValuePair[1];

			if (line.match(`${prefix}:`)) {
				const regexExtractSuffix = new RegExp(`(?<=${prefix}:)\\S+`);
				const suffix = regexExtractSuffix.exec(line);

				// Add suffix to the prefix value.
				// Since it always end with the closing tag > we will simply replace it with {SUFFIX}/>
				const prefixInserted = value.replace(">", `${suffix}>`).trim();
				formattedLine = formattedLine.replace(
					new RegExp(`${prefix}:${suffix}`, "g"),
					prefixInserted
				);
			}
		}
		return formattedLine;
	};

	// Build the Binary Tree from the elements of the Cytoscape graph recursively.
	// Input:
	//   data: Array containing all nodes and edges of the cytoscape graph
	//   nodeData: data of the node which is observed during the recursion
	buildTreeFromCyData = (data, nodeData) => {
		if (!nodeData) {
			return;
		}

		let node: Leaf | InnerNode;
		if (nodeData.type === "Leaf") {
			const { tpf, label, ...otherProperties } = nodeData;
			node = new Leaf(label, tpf, otherProperties);
		} else {
			// Inner Node
			const { joinOperator, ...otherProperties } = nodeData;
			node = new InnerNode(joinOperator, otherProperties);
		}

		if (!this.root) {
			this.root = node;
		}

		// Get children of the observed node by finding the outgoing edges from the node.
		const edgesToChildren = data.filter((el) => {
			return el.source === nodeData.id;
		});
		const children = edgesToChildren.map((edge) => {
			return data
				.filter((el) => {
					return el.id === edge.target;
				})
				.pop();
		});

		if (children.length === 2) {
			node.left = this.buildTreeFromCyData(data, children[0]);
			node.right = this.buildTreeFromCyData(data, children[1]);
		} else if (children.length === 1) {
			node.left = this.buildTreeFromCyData(data, children[0]);
		}
		return node;
	};

	// Represent the binary tree as JSON according to the format of the backend server.
	toJSON = () => {
		const result = this.generateDictionary(this.root);
		const sanitized = deleteEmptyFields(result);
		return sanitized;
	};

	// Represent the Binary Tree as JSON
	generateDictionary = (node) => {
		if (!node) {
			return;
		}

		const data = {
			type: node.type === "InnerNode" ? node.joinOperator : node.type, // Either the join type as type or "Leaf"
			tpf: node.tpf,
			cardinality: node.cardinality,
			estimated_cardinality: node.estimated_cardinality,
			left: this.generateDictionary(node.left),
			right: this.generateDictionary(node.right),
		};
		return data;
	};
}

interface INodeData {
	id: string;
	label: string;
	[key: string]: any;
}

class NodeNew {
	id: string;
	label: string;
	propertyKeys: string[];
	constructor(label: string, propertyKeys?: string[]) {
		this.id = uuidv4();
		this.label = label;
		this.propertyKeys = ["id", "label"];
		if (propertyKeys) {
			this.propertyKeys.concat(propertyKeys);
		}
	}

	getCyData(): INodeData {
		const nodeData = {};
		for (const key of this.propertyKeys) {
			nodeData[key] = this[key];
		}
		return nodeData as INodeData;
	}
}

class InnerNode extends NodeNew {
	type: "InnerNode";
	joinOperator: string;
	left?: InnerNode | Leaf;
	right?: InnerNode | Leaf;
	estimated_tuples?: number;
	produced_tuples?: number;
	[key: string]: any;

	constructor(joinOperator: string, properties: any) {
		super(joinOperator);

		this.type = "InnerNode";
		this.joinOperator = joinOperator;

		this.propertyKeys.push("type", "joinOperator");

		for (const [key, value] of Object.entries(properties)) {
			if (value !== null || value !== undefined) {
				this[key] = value;
				this.propertyKeys.push(key);
			}
		}
	}

	getCyData(): INodeData {
		const nodeData = {};
		const excludeProperties = ["left", "right"];
		for (const key of this.propertyKeys) {
			// skip excluded properties
			if (excludeProperties.includes(key)) {
				continue;
			}

			// Special cases when a property should be excluded
			switch (key) {
				case "produced_tuples":
					if (this.produced_tuples !== -1) {
						nodeData[key] = this[key];
					}
					break;
				default:
					nodeData[key] = this[key];
			}
		}
		return nodeData as INodeData;
	}
}

class Leaf extends NodeNew {
	type: "Leaf";
	tpf: string;
	[key: string]: any;

	constructor(label: string, tpf: string, properties: any) {
		super(label);
		this.type = "Leaf";
		this.tpf = tpf;

		this.propertyKeys.push("type", "tpf");

		for (const [key, value] of Object.entries(properties)) {
			if (value !== null || value !== undefined) {
				this[key] = value;
				this.propertyKeys.push(key);
			}
		}
	}
}

export default BinaryTree;
