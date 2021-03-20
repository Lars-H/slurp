const defaultConfig = {
	menuRadius: function () {
		return 75;
	}, // the outer radius (node center to the end of the menu) in pixels. It is added to the rendered size of the node. Can either be a number or function as in the example.
	// selector TO FILL
	// commands TO FILL
	fillColor: "rgba(0, 0, 0, 0.75)", // the background colour of the menu
	activeFillColor: "rgba(1, 105, 217, 0.75)", // the colour used to indicate the selected command
	activePadding: 5, // additional size in pixels for the active command
	indicatorSize: 24, // the size in pixels of the pointer to the active command, will default to the node size if the node size is smaller than the indicator size,
	separatorWidth: 1, // the empty spacing in pixels between successive commands
	spotlightPadding: 4, // extra spacing in pixels between the element and the spotlight
	adaptativeNodeSpotlightRadius: false, // specify whether the spotlight radius should adapt to the node size
	minSpotlightRadius: 24, // the minimum radius in pixels of the spotlight (ignored for the node if adaptativeNodeSpotlightRadius is enabled but still used for the edge & background)
	maxSpotlightRadius: 24, // the maximum radius in pixels of the spotlight (ignored for the node if adaptativeNodeSpotlightRadius is enabled but still used for the edge & background)
	openMenuEvents: "cxttapstart taphold", // space-separated cytoscape events that will open the menu; only `cxttapstart` and/or `taphold` work here
	itemColor: "white", // the colour of text in the command's content
	itemTextShadowColor: "transparent", // the text shadow colour of the command's content
};

const getCommands = (updateEditHistory, mode) => {
	if (mode === "NLJ") {
		return [
			{
				fillColor: "rgba(200, 200, 200, 0.75)", // optional: custom background color for item
				content: "SHJ", // html/text content to be displayed in the menu
				contentStyle: {}, // css key:value pairs to set the command's css in js if you want
				select: async function (ele) {
					await ele.data("type", "SHJ");
					await ele.data("label", "SHJ");
					updateEditHistory("push");
				},
				enabled: true,
			},
			{
				fillColor: "rgba(200, 200, 200, 0.75)",
				content: "NLJ",
				contentStyle: {},
				select: async function (ele) {
					await ele.data("type", "NLJ");
					await ele.data("label", "NLJ");
					updateEditHistory("push");
				},
				enabled: false,
			},
		];
	}
	if (mode === "SHJ") {
		return [
			{
				fillColor: "rgba(200, 200, 200, 0.75)", // optional: custom background color for item
				content: "SHJ", // html/text content to be displayed in the menu
				contentStyle: {}, // css key:value pairs to set the command's css in js if you want
				select: async function (ele) {
					await ele.data("type", "SHJ");
					await ele.data("label", "SHJ");
					updateEditHistory("push");
				},
				enabled: false,
			},
			{
				fillColor: "rgba(200, 200, 200, 0.75)",
				content: "NLJ",
				contentStyle: {},
				select: async function (ele) {
					await ele.data("type", "NLJ");
					await ele.data("label", "NLJ");
					updateEditHistory("push");
				},
				enabled: true,
			},
		];
	}

	// ELSE:

	return [
		{
			fillColor: "rgba(200, 200, 200, 0.75)", // optional: custom background color for item
			content: "SHJ", // html/text content to be displayed in the menu
			contentStyle: {}, // css key:value pairs to set the command's css in js if you want
			select: async function (ele) {
				await ele.data("type", "SHJ");
				await ele.data("label", "SHJ");
				updateEditHistory("push");
			},
			enabled: false,
		},
		{
			fillColor: "rgba(200, 200, 200, 0.75)",
			content: "NLJ",
			contentStyle: {},
			select: async function (ele) {
				await ele.data("type", "NLJ");
				await ele.data("label", "NLJ");
				updateEditHistory("push");
			},
			enabled: false,
		},
	];
};

export const nodeMenu = (updateEditHistory, mode) => {
	let selector;
	if (mode === "subtrees") {
		selector = 'node[type != "Leaf"][?NLJdisabled]';
	} else if (mode === "NLJ") {
		selector = 'node[joinOperator = "NLJ"][!NLJdisabled]';
	} else if (mode === "SHJ") {
		selector = 'node[joinOperator = "SHJ"][!NLJdisabled]';
	}

	return {
		...defaultConfig,
		selector: selector,
		commands: getCommands(updateEditHistory, mode),
	};
};

export const coreMenu = (buildFromScratch, resetChanges, undoLastAction, editHistory) => {
	// Disable undo action if there is no action to be undone
	let undoEnabled;
	if (!editHistory || editHistory.length < 2) {
		undoEnabled = false;
	} else {
		undoEnabled = true;
	}

	return {
		menuRadius: function () {
			return 90;
		},
		selector: "core",
		commands: [
			{
				fillColor: "rgba(200, 200, 200, 0.75)",
				content: "from scratch",
				contentStyle: {},
				select: function () {
					buildFromScratch();
				},
				enabled: true,
			},
			{
				fillColor: "rgba(200, 200, 200, 0.75)",
				content: "reset",
				contentStyle: {},
				select: function () {
					resetChanges();
				},
				enabled: true,
			},
			{
				fillColor: "rgba(200, 200, 200, 0.75)",
				content: "undo",
				contentStyle: {},
				select: function () {
					undoLastAction();
				},
				enabled: undoEnabled, // whether the command is selectable
			},
		],
		fillColor: "rgba(0, 0, 0, 0.75)",
		activeFillColor: "rgba(1, 105, 217, 0.75)",
		activePadding: 5,
		indicatorSize: 24,
		separatorWidth: 3,
		spotlightPadding: 4,
		adaptativeNodeSpotlightRadius: false,
		minSpotlightRadius: 24,
		maxSpotlightRadius: 24,
		openMenuEvents: "cxttapstart taphold",
		itemColor: "white",
		itemTextShadowColor: "transparent",
	};
};
