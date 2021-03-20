const cyStyleLight: cytoscape.Stylesheet[] = [
	{
		selector: "node[label]",
		style: {
			label: "data(label)",
		},
	},
	{
		selector: ".center-center",
		style: {
			"text-valign": "center",
			"text-halign": "center",
		},
	},
	{
		selector: "node",
		style: {
			width: 64,
			height: 64,
			"background-color": "#E2E8F0",
			color: "black",
		},
	},
	{
		selector: "edge",
		style: {
			width: 4,
			"overlay-opacity": 0,
			"line-color": "#999999",
		},
	},
	{
		selector: "edge:selected",
		style: {
			"line-color": "#999999",
		},
	},
	{
		selector: "node.selected-for-swap",
		style: {
			"background-color": "#90CDF4",
		},
	},
	{
		selector: "node[type != 'Leaf']",
		style: {
			shape: "round-diamond",
			width: 74,
			height: 74,
		},
	},
];

export default cyStyleLight;
