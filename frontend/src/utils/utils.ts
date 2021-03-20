/* eslint-disable no-prototype-builtins */
export const deleteEmptyFields = (obj: any) => {
	Object.keys(obj).forEach((key) => {
		if (obj[key] && typeof obj[key] === "object") deleteEmptyFields(obj[key]);
		else if (obj[key] === undefined || obj[key] === null) delete obj[key];
	});
	return obj;
};

export const convertSpecialCharsToHTML = (s: string) => {
	const el = document.createElement("div");
	el.innerText = el.textContent = s;
	s = el.innerHTML;
	return s;
};

export const timeConverter = (UNIX_timestamp) => {
	const a = new Date(UNIX_timestamp * 1000);
	const months = [
		"Jan",
		"Feb",
		"Mar",
		"Apr",
		"May",
		"Jun",
		"Jul",
		"Aug",
		"Sep",
		"Oct",
		"Nov",
		"Dec",
	];
	const year = a.getFullYear();
	const month = months[a.getMonth()];
	const date = a.getDate();
	const hour = a.getHours();
	const min = a.getMinutes();
	const sec = a.getSeconds();
	const time = date + " " + month + " " + year + " " + hour + ":" + min + ":" + sec;
	return time;
};

export const formatQuery = (query: string) => {
	// Delete all line breaks
	query = query.replace(/(\r\n|\n|\r)/gm, "");

	// Replace all white space types by simple single white space
	query = query.replaceAll(/\s+/g, " ");

	// Seperate triple patterns without whitespaces by whitespace

	query = query.replaceAll(".<", ". <");
	query = query.replaceAll('./"', ". /");
	query = query.replaceAll(".}", ". }");
	query = query.replaceAll(".?", ". ?");

	// // Add line breaks after triple patterns
	query = query.split(". ").join(".\n	");

	// // Add line break after SELECT WHERE {
	query = query.replace(/select/i, "SELECT");
	query = query.replace(/where/i, "WHERE");
	query = query.replaceAll(/(prefix.*?>)/gi, "$1\n");
	query = query.replaceAll(/prefix/gi, "PREFIX");
	query = query.replaceAll(/ prefix/gi, "PREFIX");
	query = query.replace("SELECT*", "SELECT *");
	query = query.replace("*WHERE", "* WHERE");
	query = query.replace(" SELECT", "SELECT");
	query = query.replace(">SELECT", ">\nSELECT");
	query = query.replace("WHERE{", "WHERE {\n	");
	query = query.replace("WHERE {", "WHERE {\n	");
	query = query.replaceAll(".\n", " .\n");
	query = query.replaceAll("  .\n", " .\n");
	query = query.replaceAll(". ", ".");
	query = query.replaceAll("	 ", "	");
	query = query.replace("	}", "}");
	query = query.replace(/(^[ \t]*\n)/gm, "");

	return query;
};

// export const validURL = (str) => {
//   const pattern = new RegExp('^(https?:\\/\\/)?'+ // protocol
//     '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|'+ // domain name
//     '((\\d{1,3}\\.){3}\\d{1,3}))'+ // OR ip (v4) address
//     '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*'+ // port and path
//     '(\\?[;&a-z\\d%_.~+=-]*)?'+ // query string
//     '(\\#[-a-z\\d_]*)?$','i'); // fragment locator
//   return !!pattern.test(str);
// }

// https://stackoverflow.com/questions/1068834/object-comparison-in-javascript
export function deepCompare(x: any, y: any) {
	let i, l, leftChain, rightChain;

	function compare2Objects(x: any, y: any) {
		let p;

		// remember that NaN === NaN returns false
		// and isNaN(undefined) returns true
		if (isNaN(x) && isNaN(y) && typeof x === "number" && typeof y === "number") {
			return true;
		}

		// Compare primitives and functions.
		// Check if both arguments link to the same object.
		// Especially useful on the step where we compare prototypes
		if (x === y) {
			return true;
		}

		// Works in case when functions are created in constructor.
		// Comparing dates is a common scenario. Another built-ins?
		// We can even handle functions passed across iframes
		if (
			(typeof x === "function" && typeof y === "function") ||
			(x instanceof Date && y instanceof Date) ||
			(x instanceof RegExp && y instanceof RegExp) ||
			(x instanceof String && y instanceof String) ||
			(x instanceof Number && y instanceof Number)
		) {
			return x.toString() === y.toString();
		}

		// At last checking prototypes as good as we can
		if (!(x instanceof Object && y instanceof Object)) {
			return false;
		}

		if (x.isPrototypeOf(y) || y.isPrototypeOf(x)) {
			return false;
		}

		if (x.constructor !== y.constructor) {
			return false;
		}

		if (x.prototype !== y.prototype) {
			return false;
		}

		// Check for infinitive linking loops
		if (leftChain.indexOf(x) > -1 || rightChain.indexOf(y) > -1) {
			return false;
		}

		// Quick checking of one object being a subset of another.
		// todo: cache the structure of arguments[0] for performance
		for (p in y) {
			if (y.hasOwnProperty(p) !== x.hasOwnProperty(p)) {
				return false;
			} else if (typeof y[p] !== typeof x[p]) {
				return false;
			}
		}

		for (p in x) {
			if (y.hasOwnProperty(p) !== x.hasOwnProperty(p)) {
				return false;
			} else if (typeof y[p] !== typeof x[p]) {
				return false;
			}

			switch (typeof x[p]) {
				case "object":
				case "function":
					leftChain.push(x);
					rightChain.push(y);

					if (!compare2Objects(x[p], y[p])) {
						return false;
					}

					leftChain.pop();
					rightChain.pop();
					break;

				default:
					if (x[p] !== y[p]) {
						return false;
					}
					break;
			}
		}

		return true;
	}

	if (arguments.length < 1) {
		return true; //Die silently? Don't know how to handle such case, please help...
		// throw "Need two or more arguments to compare";
	}

	for (i = 1, l = arguments.length; i < l; i++) {
		leftChain = []; //Todo: this can be cached
		rightChain = [];

		// eslint-disable-next-line prefer-rest-params
		if (!compare2Objects(arguments[0], arguments[i])) {
			return false;
		}
	}

	return true;
}
