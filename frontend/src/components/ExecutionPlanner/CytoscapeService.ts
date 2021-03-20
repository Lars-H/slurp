import cloneDeep from "lodash/cloneDeep";
import { v4 as uuidv4 } from "uuid";
import { logger } from "utils/logger";
import tippy from "tippy.js";
import "tippy.js/dist/tippy.css";
import { nodeMenu, coreMenu } from "./cy-cxtmenu";
import { convertSpecialCharsToHTML } from "../../utils/utils";
import BinaryTree from "utils/DataStructures/binaryTree";
import { InvalidExecutionPlanError } from "./InvalidExecutionPlanError";
import {
	CollectionReturnValue,
	EventObject,
	EventObjectNode,
	NodeCollection,
	NodeSingular,
	Position,
} from "cytoscape";

const MODE_EDIT = "edit";
const MODE_VIEW = "view";
const NODE_SWAP_TIME_LIMIT = 4000;

const ADDITIONAL_CONTAINER_SPACE = 50;

export class CytoscapeService {
	cy!: cytoscape.Core;
	mode;
	layout;

	subgraphs;
	nodeToSwap;

	editHistory;

	cxtNodeMenu;
	cxtCoreMenu: any;
	tip;
	maxWidth!: number;
	maxHeight!: number;
	adjustHeightCallback!: (height: string) => void;
	initialExecutionPlan!: cytoscape.ElementDefinition[];
	doubleClickTimeout?: NodeJS.Timeout;

	grabbedNodePrevX!: number;
	grabbedNodePrevY!: number;

	grabbedGraph!: NodeCollection;

	/**
	 * Called by react component that creates the cytoscape instance which creates another headless cytoscape instance.
	 *
	 * @param cy cytoscape instance. Can be headed or headless (check via !!cy.container()). This
	 * class should work with both headed an headless cy instances.
	 *
	 * @param mode mode of cytoscape. Two possible values:
	 *   - "edit": Allows user to modify the graph.
	 *   - "view": No interaction with graph allowed
	 *
	 * @param initialExecutionPlan the suggested exection plan from query planner. Is needed in order to
	 * reset the graph to its initial state.
	 *
	 * @param adjustHeightCallback callback to adjust the height of the react component.
	 */
	registerCytoscapeInstance(
		cy: cytoscape.Core,
		mode: "edit" | "view",
		initialExecutionPlan: cytoscape.ElementDefinition[],
		adjustHeightCallback: (height: string) => void,
		maxWidth?: number
	) {
		if (this.cy === cy) {
			return;
		}

		this.cy = cy;
		this.mode = mode;
		this.adjustHeightCallback = adjustHeightCallback;
		this.initialExecutionPlan = initialExecutionPlan;

		this.layout = {
			name: "dagre",
			fit: false,
			transform: function (node, pos: Position) {
				// Add padding
				const padding = mode === MODE_EDIT ? ADDITIONAL_CONTAINER_SPACE / 2 : 0;
				return {
					x: pos.x + padding,
					y: pos.y + padding,
				};
			},
		};

		this.updateCyContainer();

		// Show info on hover
		cy.on("mouseover", "node", this.handleOnHover);
		cy.on("mouseout", "node", this.handleOnHoverOff);

		// Disable interaction with the graph
		if (this.mode === MODE_VIEW) {
			this.cy.nodes().ungrabify();
		}

		if (this.mode === MODE_EDIT) {
			// TODO: DESCRIBE
			if (maxWidth) {
				this.maxWidth = maxWidth;
			}

			this.cxtCoreMenu = cy.cxtmenu(
				coreMenu(
					this.buildFromScratch,
					this.resetChanges,
					this.undoLastAction,
					this.editHistory
				)
			);

			// No variable needed because menues are overwritten
			cy.cxtmenu(nodeMenu(this.updateEditHistory, "subtrees"));
			cy.cxtmenu(nodeMenu(this.updateEditHistory, "NLJ"));
			cy.cxtmenu(nodeMenu(this.updateEditHistory, "SHJ"));

			cy.dblclick();

			// disable selection of multiple nodes at once
			cy.on("select", "node, edge", (e) => cy.elements().not(e.target).unselect());

			cy.on("grab", "node", this.handleGrab);
			cy.on("drag", "node", this.handleDrag);
			cy.on("free", "node", this.handle_free);
			cy.on("dblclick", "node[type='Leaf']", this.handleDoubleClick);

			if (!this.editHistory) {
				this.editHistory = [this.cy.json()];
			}
		}
	}

	// Called when component running this service is unmountent.
	unregisterService = () => {
		if (this.tip) {
			this.tip.destroy();
		}
	};

	/**
	 * The cytoscape graph has to updated after each graph manipulation:
	 * 1) Retrieve the new subgraphs
	 * 2) Calculate their position by applying the layout
	 * 3) Dynamically adjust the height of the graph
	 * 4) Reset unfinished actions
	 */
	updateCyContainer = async () => {
		this.updateSubgraphs();
		await this.cy.makeLayout(this.layout).run(); // Rerun layout to reorder the elements
		this.adjustHeightCallback(this.calcCytoscapeContainerHeight());
		this.resetSelectedNodeForSwap();
	};

	/**
	 * Method is necessary because this.cy.width() (and height()) returned wrong results.
	 * Therefore we measure the width of the canvas in the DOM after it has been rendered.
	 * @param width. new width of the cytoscape canvas after the resize
	 */
	updateMaxWidth = (width) => {
		this.maxWidth = width;
		for (const graph of this.subgraphs) {
			if (graph.renderedBoundingbox().x2 > this.maxWidth) {
				this.cy.makeLayout(this.layout).run(); // Rerun layout to reorder the elements
			}
		}
	};

	/**
	 * Test if @param node has two subtrees with more than one element as children.
	 */
	hasTwoSubtreesWithMoreThanOneElementAsChildren = (node) => {
		const childNodes = node.outgoers().nodes();
		if (childNodes.length === 2) {
			const subtreesAsChildren = childNodes.filter(
				(child) => child.outgoers().nodes().length === 2
			);
			if (subtreesAsChildren.length === 2) {
				return true;
			}
		}
		return false;
	};

	/**
	 * Calculates the new subgraphs after graph manipulation
	 * (e.g. if two graphs were merged or a graph was split up)
	 */
	updateSubgraphs = () => {
		const elements = this.cy.elements().clone();
		const subgraphs: CollectionReturnValue[] = [];

		let unvisitedNodesIDs: string[] = [];

		const nodes = elements.nodes();
		for (let i = 0; i < nodes.length; i++) {
			unvisitedNodesIDs.push(nodes[i].data("id"));
		}

		while (unvisitedNodesIDs.length > 0) {
			const observedNode = this.cy.getElementById(unvisitedNodesIDs[0]);
			let subgraph = this.cy.collection();

			this.cy.elements().bfs({
				roots: observedNode,
				visit: (v) => {
					subgraph = subgraph.union(v);
					if (this.hasTwoSubtreesWithMoreThanOneElementAsChildren(v)) {
						// Nested Loop Join is not allowed in two subtrees where each has more than one element.
						// This field is used as a selector for a different ctxmenu where NLJ is disabled
						v.data("NLJdisabled", true);
					} else {
						v.data("NLJdisabled", false);
					}
				},
			});

			const visitedNodesIDs: any[] = [];
			subgraph.forEach(function (ele) {
				visitedNodesIDs.push(ele.data("id"));
			});

			unvisitedNodesIDs = unvisitedNodesIDs.filter((el) => !visitedNodesIDs.includes(el));

			const edges = subgraph.edgesWith(subgraph);
			// TODO
			// subgraph = this.cy.elements(subgraph.union(edges));
			subgraph = subgraph.union(edges);

			subgraphs.push(subgraph);
		}

		this.subgraphs = subgraphs;
	};

	resetSelectedNodeForSwap = () => {
		if (this.nodeToSwap) {
			this.cy.elements().removeClass("selected-for-swap");
			this.nodeToSwap = null;
		}
	};

	calcCytoscapeContainerHeight = () => {
		let maxHeightSubtree = 0;
		// provide more space for the edit mode
		const additionalHeight = this.mode === MODE_EDIT ? ADDITIONAL_CONTAINER_SPACE : 0;

		// Iterate through all subtrees
		for (const subgraph of this.subgraphs) {
			const boundingBox = subgraph.renderedBoundingbox();
			const heightSubtree = boundingBox.h;
			if (heightSubtree > maxHeightSubtree) {
				maxHeightSubtree = heightSubtree;
			}
		}
		const newHeight = maxHeightSubtree + additionalHeight;
		// TODO
		this.maxHeight = newHeight;
		return `${newHeight}px`;
	};

	/**
	 * Event is triggered if a leaf (tripple pattern) was double clicked.
	 * A double click initiates the Leaf Swapping process.
	 */
	handleDoubleClick = (evt: EventObject) => {
		if (this.doubleClickTimeout) {
			clearTimeout(this.doubleClickTimeout);
		}

		this.doubleClickTimeout = setTimeout(() => {
			if (this.nodeToSwap) {
				this.nodeToSwap.removeClass("selected-for-swap");
				this.nodeToSwap = null;
			}
		}, NODE_SWAP_TIME_LIMIT);

		const target = evt.target;
		this.nodeToSwap = target;
		target.addClass("selected-for-swap");
	};

	/** 
		Create HTML Content for the generated Tippy on hover of a node. 
		Content is generic since it renders all key, value pairs which it receives from the backend (except default keys like id etc.)
	*/
	createTippyContent = (node: NodeSingular) => {
		const ignoreKeys = ["id", "label", "type", "NLJdisabled", "joinOperator"];
		const content = document.createElement("div");
		for (const [key, value] of Object.entries(node.data())) {
			// Do not show values of those keys.
			if (ignoreKeys.includes(key)) {
				continue;
			}
			const child = document.createElement("div");

			// Check for known jeys with different wording.
			switch (key) {
				case "cardinality":
					if (node.data().type === "Leaf") {
						child.innerHTML = `<b>Cardinality</b>: ${value}<br/>`;
					} else {
						// Cardinality has an other meaning on Inner Nodes
						child.innerHTML = `<b>Join Cardinality</b>: ${value}<br/>`;
					}
					break;
				case "estimated_tuples":
					child.innerHTML = `<b>Estimated Join Cardinality</b>: ${value}<br/>`;
					break;
				case "tpf":
					// TODO RENAME TO TP
					child.innerHTML = `<b>TP</b>: ${convertSpecialCharsToHTML(value as string)}`;
					break;
				default:
					if (typeof value === "string") {
						child.innerHTML = `<b>${key}</b>: ${convertSpecialCharsToHTML(value)}<br/>`;
					} else if (typeof value === "object") {
						// do not show objects in tippy element
						continue;
					} else {
						child.innerHTML = `<b>${key}</b>: ${value}<br/>`;
					}
			}
			content.appendChild(child);
		}
		return content;
	};

	/**
	 * Event is triggered after the user has hover over a leaf (Tripple Pattern).
	 * Creates a Tippy tooltip containing information about the leaf.
	 */
	handleOnHover = (evt: EventObject) => {
		const node = evt.target;
		const ref = node.popperRef(); // used only for positioning

		// unfortunately, a dummy element must be passed as tippy only accepts a dom element as the target
		// https://github.com/atomiks/tippyjs/issues/661
		const dummyDomEle = document.createElement("div");

		// using tippy@^5.2.1
		const tip = tippy(dummyDomEle, {
			trigger: "manual",
			lazy: false,
			onCreate: (instance) => {
				if (instance.popperInstance) {
					instance.popperInstance.reference = ref;
				}
			}, // needed for `ref` positioning
			maxWidth: 500,
			content: () => {
				return this.createTippyContent(node);
			},
		});
		tip.show();
		this.tip = tip;
	};

	handleOnHoverOff = () => {
		if (this.tip) {
			this.tip.destroy();
		}
	};

	/**
	 * Updates position of the whole node's subgraph which is currently dragged.
	 */
	handleDrag = (evt: EventObjectNode) => {
		// TODO Dont allow dragging outsoude canvas
		const grabbedNode = evt.target;

		const deltaX = grabbedNode.position().x - this.grabbedNodePrevX;
		const deltaY = grabbedNode.position().y - this.grabbedNodePrevY;

		const nodesOutsideRenderingBox: NodeSingular[] = [];

		for (let i = 0; i < this.grabbedGraph.length; i++) {
			const node = this.grabbedGraph[i];
			const nodeX = node.position().x + deltaX;
			const nodeY = node.position().y + deltaY;

			if (nodeX > this.maxWidth || nodeX < 0 || nodeY > this.maxHeight || nodeY < 0) {
				nodesOutsideRenderingBox.push(node);
			}
		}

		if (nodesOutsideRenderingBox.length === 0) {
			for (let i = 0; i < this.grabbedGraph.length; i++) {
				const node = this.grabbedGraph[i];
				if (node !== grabbedNode) {
					node.position({
						x: node.position().x + deltaX,
						y: node.position().y + deltaY,
					});
				}
			}
		} else {
			grabbedNode.position({
				x: this.grabbedNodePrevX,
				y: this.grabbedNodePrevY,
			});
		}

		this.grabbedNodePrevX = grabbedNode.position().x;
		this.grabbedNodePrevY = grabbedNode.position().y;
	};

	/**
	 * Event is triggered if the user has grabbed (clicked) a node.
	 *
	 * If the user has previously double clicked a leaf in order to swap it, then a leaf swap is performed
	 * Otherwise, if the user grabs a node in order to drag it afterwards, then the drag is initialized.
	 */
	handleGrab = (evt: EventObject) => {
		const grabbedNode = evt.target;

		if (this.isNodeSwapIntended(grabbedNode, this.nodeToSwap)) {
			this.swapTwoLeaves(this.nodeToSwap, grabbedNode);
			return;
		}

		// Remember initial Point of grab
		this.grabbedNodePrevX = grabbedNode.position().x;
		this.grabbedNodePrevY = grabbedNode.position().y;

		const nodes: NodeSingular[] = [];

		let subgraph = this.cy.collection();

		this.cy.elements().bfs({
			roots: grabbedNode,
			visit: function (v) {
				nodes.push(v);
				subgraph = subgraph.union(v);
			},
		});

		this.grabbedGraph = subgraph;
	};

	/**
	 * Check if a graph/node was dragged on an other subgraph/node.
	 * If so, then merge these.
	 */
	handle_free = (evt: EventObject) => {
		const targetRenderedPosition = evt.target.renderedPosition();

		let subgraphOfTarget;
		const subgraphsWithoutTarget: any[] = [];

		// Get subgraph of the dragged element and the subgraphs without the element
		// Assumption: Each element can only be in one subgraph
		for (const subgraph of this.subgraphs) {
			if (subgraph.getElementById(evt.target.id()).length === 0) {
				subgraphsWithoutTarget.push(subgraph);
			} else {
				subgraphOfTarget = subgraph;
			}
		}

		// Subgraph of the dragged element is excluded because otherwise each drag would cause a merge
		for (const subgraph of subgraphsWithoutTarget) {
			const subgraphRenderedBoundingbox = subgraph.renderedBoundingbox();
			if (this.isInBoundingBox(targetRenderedPosition, subgraphRenderedBoundingbox)) {
				this.mergeTrees(subgraphOfTarget, subgraph);

				// During the merge the layout is reloaded for maintaining the tree alignment
				// Due to realinging there can be more than one match. Therefore the loop is terminated after the first match (only one exists before alignment)
				break;
			}
		}
	};

	isSingleNode = (node) => {
		return node.connectedEdges().empty();
	};

	sameParentNode = (leafOne, leafTwo) => {
		const edgeOne = leafOne.connectedEdges();
		const edgeTwo = leafTwo.connectedEdges();
		if (!edgeOne.empty() && !edgeTwo.empty()) {
			if (edgeOne.data().source === edgeTwo.data().source) {
				return true;
			}
		}
		return false;
	};

	// A swap of two leaves can be imagined as follows: You take the edges of each node and simply swap the targets of the edges.
	// Since the edges' targets are immutable in the cytoscape library, the edges have to be replaced instead in order to execute a swap.
	swapTwoLeaves = (firstLeaf, secondLeaf) => {
		const first = firstLeaf;
		const firstEdge = first.connectedEdges();
		first.removeClass("selected-for-swap");
		const second = secondLeaf;
		const secondEdge = second.connectedEdges();

		/**
		 * The modified edges will be added to the end of the array containing all cytoscape elements due to the replacement.
		 * The 'dagre' layout (extension) which is used for arranging elements to display and format trees, builds the trees by iterating over the array in order.
		 * This causes the following problem:
		 * If we intend two swap the leaves X1 and X2 it will lead to the following result:
		 *         NLJ                            NLJ
		 *         / \                            / \
		 *       NLJ  X1     RESULTS IN         NLJ  X2
		 *       / \                            / \
		 *      X2  x3                         x3 X1
		 * This problem only occurs if you want to swap the left leaf of a node which has two leaves as its children.
		 * This happens because the edge from {NLJ to x3} is positioned before the edge {NLJ to X1} in the cytoscape array.
		 * As soon as the layout arrives at the edge {NLJ to X1}, x3 is alreay registered as the first child of the according node NLJ.
		 * The first registered child of a node is always positioned left in this layout.
		 * Although this problem is indifferent for the execution of the query, it is still tremendous because this might harm the user experience and has therefore to be fixed.
		 *
		 * Since we intend to further use this layout (extension) because it solves the layout problem, we have to find a workaround.
		 * Therefore we decided to remove edges like {NLJ to x3} and add them back to the end of the array after the intended swap has been done.
		 */
		const edgeToRemove = this.findEdgesToReplaceForMaintainingOrder(first, second);

		// XOR operation. True when one leaf is a single node and the other leaf is part of a bigger tree (more than 1 node)
		if (this.isSingleNode(first) !== this.isSingleNode(second)) {
			if (!firstEdge.empty()) {
				this.cy.remove(firstEdge);
				this.cy.add({
					data: {
						id: firstEdge.data("id"),
						source: firstEdge.data("source"),
						target: second.data("id"),
					},
					group: "edges",
				});
			}
			if (!secondEdge.empty()) {
				this.cy.remove(secondEdge);
				this.cy.add({
					data: {
						id: secondEdge.data("id"),
						source: secondEdge.data("source"),
						target: first.data("id"),
					},
					group: "edges",
				});
			}
		} else {
			// The remaining two cases, which are handled here, are:
			//    1) two leaves from the same tree are swapped
			//    2) one single node is swapped with a leaf from a tree with more than 1 node
			this.cy.remove(firstEdge);
			this.cy.remove(secondEdge);

			// First edge, target replaced
			this.cy.add({
				data: {
					id: firstEdge.data("id"),
					source: firstEdge.data("source"),
					target: second.data("id"),
				},
				group: "edges",
			});

			// Second edge, target replaced
			this.cy.add({
				data: {
					id: secondEdge.data("id"),
					source: secondEdge.data("source"),
					target: first.data("id"),
				},
				group: "edges",
			});
		}

		if (edgeToRemove) {
			this.cy.remove(edgeToRemove);
			this.cy.add(edgeToRemove);
		}

		this.nodeToSwap = null;

		this.updateCyContainer();
		this.updateEditHistory("push");
	};

	mergeTrees = (tree1, tree2) => {
		// get root of each tree
		const root1 = tree1.roots();
		const root2 = tree2.roots();

		// If two subtrees with each more than one node are joined, then the JOIN is a SHJ, otherwise a NLJ
		const joinID = uuidv4();
		const joinOperator = {
			id: joinID,
			type: tree1.length > 1 && tree2.length > 1 ? "SHJ" : "NLJ",
			label: tree1.length > 1 && tree2.length > 1 ? "SHJ" : "NLJ",
		};
		this.cy.add({
			data: joinOperator,
			group: "nodes",
			classes: "center-center",
		});

		// create edges between join and trees
		this.cy.add([
			{
				group: "edges",
				data: { source: joinID, target: root1.data("id") },
			},
			{
				group: "edges",
				data: { source: joinID, target: root2.data("id") },
			},
		]);

		// update subgraphs for further operations
		this.updateCyContainer();
		this.updateEditHistory("push");
	};

	// IMPORTANT: Motivation see comment in the method swapTwoLeaves
	findEdgesToReplaceForMaintainingOrder = (firstLeaf, secondLeaf) => {
		const first = firstLeaf;
		const firstEdge = first.connectedEdges();
		const second = secondLeaf;
		const secondEdge = second.connectedEdges();

		let edgesToRemove = this.cy.collection();

		// Check for both edges because in the following scenario both edges have to be added to the end of the cytoscape elements array:
		//    NLJ         NLJ
		//  ->/ \       ->/ \
		//   X1  X2      X3 X4
		for (const edge of [firstEdge, secondEdge]) {
			if (edge.empty()) {
				continue;
			}

			const parentOfTwoLeaves = this.cy.getElementById(edge.data().source);
			const leavesOfParent = parentOfTwoLeaves.outgoers("node[type='Leaf']");

			// Problem with wrong order occurs only if one of the two nodes to be swapped is child of a node having two leaves as children.
			if (leavesOfParent.length === 2) {
				const nodeToBeSwapped = this.cy.getElementById(edge.data().target);

				// get other child of parent
				const other = leavesOfParent.subtract(nodeToBeSwapped);

				// Compare positions of leaves. If the node to be swapped is the left child then its edge has to be added to the end of the array after the swap
				if (nodeToBeSwapped.position().x < other.position().x) {
					const edgeFromOtherToSource = other.connectedEdges();
					edgesToRemove = edgesToRemove.union(edgeFromOtherToSource);
				}
			}
		}
		return edgesToRemove;
	};

	/**
	 * Updates the edit history after the cytograph has been modified.
	 * @param mode update mode. There are two possible modes:
	 *   - "push": adds snapshot of cytoscape graph to the stack
	 *   - "reset": resets TODO
	 */
	updateEditHistory = (mode) => {
		if (mode === "push") {
			this.editHistory = [...this.editHistory, this.cy.json()];
		} else if (mode === "reset") {
			this.editHistory = [this.cy.json()];
		}
		this.updateCxtMenu("core");
	};

	isLeaf = (node) => {
		logger(node.data("type"));
		return node.data("type") === "Leaf";
	};

	isNodeSwapIntended = (grabbedNode, nodeToSwap) => {
		// TODO: show user feedback in last two cases
		return (
			nodeToSwap &&
			grabbedNode &&
			this.isLeaf(grabbedNode) &&
			this.isLeaf(nodeToSwap) &&
			grabbedNode !== nodeToSwap &&
			// Do not swap nodes if both are single nodels
			(!this.isSingleNode(grabbedNode) || !this.isSingleNode(nodeToSwap)) &&
			// Do not swap nodes if they have the same parent node (because it has no effect on the execution)
			!this.sameParentNode(nodeToSwap, grabbedNode)
		);
	};

	buildFromScratch = () => {
		const tripplePatterns = this.cy.nodes().filter('[type = "Leaf"]');
		this.cy.elements().remove();
		this.cy.add(tripplePatterns);

		this.updateCyContainer();
		this.updateEditHistory("push");
	};

	resetChanges = () => {
		this.cy.elements().remove();
		this.cy.add(cloneDeep(this.initialExecutionPlan));
		this.updateEditHistory("reset");
		this.updateCyContainer();
	};

	undoLastAction = () => {
		if (this.editHistory.length < 2) {
			return;
		}

		this.editHistory = this.editHistory.slice(0, this.editHistory.length - 1);

		this.cy.json(this.editHistory[this.editHistory.length - 1]);

		this.updateCyContainer();
		this.updateCxtMenu("core");
	};

	isInBoundingBox(target, bBox) {
		// TODO hier noch aditional padding einstellen oder so
		const bBoxLeft = bBox.x1;
		const bBoxRight = bBox.x2;
		const bBoxUpper = bBox.y1;
		const bBoxLower = bBox.y2;

		return (
			target.x >= bBoxLeft &&
			target.x <= bBoxRight &&
			target.y >= bBoxUpper &&
			target.y <= bBoxLower
		);
	}

	updateCxtMenu = (menu) => {
		if (menu === "core") {
			if (this.cxtCoreMenu) {
				this.cxtCoreMenu.destroy();
			}

			this.cxtCoreMenu = this.cy.cxtmenu(
				coreMenu(
					this.buildFromScratch,
					this.resetChanges,
					this.undoLastAction,
					this.editHistory
				)
			);
		}
	};

	getExecutionPlan = () => {
		const elements = this.cy.elements();
		const roots = elements.roots();
		// assert(roots === 1, "More than one root");

		// VALIDATE
		if (roots.length !== 1) {
			throw new InvalidExecutionPlanError(
				"Invalid execution plan",
				`The execution plan must be a single tree. Currently there are ${roots.length} subtrees.`
			);
		}
		const root: NodeSingular = roots[0];

		const data: any[] = [];
		elements.map((el) => {
			data.push(el.data());
		});
		const tree = new BinaryTree();
		tree.buildTreeFromCyData(data, root.data());
		const executionPlanJSON = tree.toJSON();

		return executionPlanJSON;
	};
}
