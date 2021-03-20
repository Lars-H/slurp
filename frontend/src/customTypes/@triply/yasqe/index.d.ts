import * as superagent from "superagent";
import { Prefixes } from "./prefixUtils";
import * as sparql11Mode from "../grammar/tokenizer";
import { Storage as YStorage } from "@triply/yasgui-utils";
import * as queryString from "query-string";
import * as Sparql from "./sparql";
import * as Autocompleter from "./autocompleters";
import CodeMirror from "./CodeMirror";
export interface Yasqe {
	on(
		eventName: "query",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest) => void
	): void;
	off(
		eventName: "query",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest) => void
	): void;
	on(
		eventName: "queryAbort",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest) => void
	): void;
	off(
		eventName: "queryAbort",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest) => void
	): void;
	on(
		eventName: "queryResponse",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest, duration: number) => void
	): void;
	off(
		eventName: "queryResponse",
		handler: (instance: Yasqe, req: superagent.SuperAgentRequest, duration: number) => void
	): void;
	showHint: (conf: HintConfig) => void;
	on(eventName: "error", handler: (instance: Yasqe) => void): void;
	off(eventName: "error", handler: (instance: Yasqe) => void): void;
	on(eventName: "blur", handler: (instance: Yasqe) => void): void;
	off(eventName: "blur", handler: (instance: Yasqe) => void): void;
	on(
		eventName: "queryResults",
		handler: (instance: Yasqe, results: any, duration: number) => void
	): void;
	off(
		eventName: "queryResults",
		handler: (instance: Yasqe, results: any, duration: number) => void
	): void;
	on(eventName: "autocompletionShown", handler: (instance: Yasqe, widget: any) => void): void;
	off(eventName: "autocompletionShown", handler: (instance: Yasqe, widget: any) => void): void;
	on(eventName: "autocompletionClose", handler: (instance: Yasqe) => void): void;
	off(eventName: "autocompletionClose", handler: (instance: Yasqe) => void): void;
	on(eventName: "resize", handler: (instance: Yasqe, newSize: string) => void): void;
	off(eventName: "resize", handler: (instance: Yasqe, newSize: string) => void): void;
	on(eventName: string, handler: () => void): void;
}
export declare class Yasqe extends CodeMirror {
	private static storageNamespace;
	autocompleters: {
		[name: string]: Autocompleter.Completer | undefined;
	};
	private prevQueryValid;
	queryValid: boolean;
	lastQueryDuration: number | undefined;
	private req;
	private queryStatus;
	private queryBtn;
	private resizeWrapper?;
	rootEl: HTMLDivElement;
	storage: YStorage;
	config: Config;
	persistentConfig: PersistentConfig | undefined;
	superagent: superagent.SuperAgentStatic;
	constructor(parent: HTMLElement, conf?: PartialConfig);
	private handleHashChange;
	private handleChange;
	private handleBlur;
	private handleChanges;
	private handleCursorActivity;
	private handleQuery;
	private handleQueryResponse;
	private handleQueryAbort;
	private registerEventListeners;
	private unregisterEventListeners;
	emit(event: string, ...data: any[]): void;
	getStorageId(getter?: Config["persistenceId"]): string | undefined;
	private drawButtons;
	private drawResizer;
	private initDrag;
	private calculateDragOffset;
	private doDrag;
	private stopDrag;
	duplicateLine(): void;
	private updateQueryButton;
	handleLocalStorageQuotaFull(_e: any): void;
	saveQuery(): void;
	getQueryType():
		| "SELECT"
		| "CONSTRUCT"
		| "ASK"
		| "DESCRIBE"
		| "INSERT"
		| "DELETE"
		| "LOAD"
		| "CLEAR"
		| "CREATE"
		| "DROP"
		| "COPY"
		| "MOVE"
		| "ADD";
	getQueryMode(): "update" | "query";
	getVariablesFromQuery(): string[];
	private autoformatSelection;
	static autoformatString(text: string): string;
	commentLines(): void;
	autoformat(): void;
	getQueryWithValues(
		values:
			| string
			| {
					[varName: string]: string;
			  }
			| Array<{
					[varName: string]: string;
			  }>
	): string;
	getValueWithoutComments(): string;
	setCheckSyntaxErrors(isEnabled: boolean): void;
	checkSyntax(): void;
	getCompleteToken(token?: Token, cur?: Position): Token;
	getPreviousNonWsToken(line: number, token: Token): Token;
	getNextNonWsToken(lineNumber: number, charNumber?: number): Token | undefined;
	private notificationEls;
	showNotification(key: string, message: string): void;
	hideNotification(key: string): void;
	enableCompleter(name: string): Promise<void>;
	disableCompleter(name: string): void;
	autocomplete(fromAutoShow?: boolean): void;
	collapsePrefixes(collapse?: boolean): void;
	getPrefixesFromQuery(): Prefixes;
	addPrefixes(prefixes: string | Prefixes): void;
	removePrefixes(prefixes: Prefixes): void;
	updateWidget(): void;
	query(config?: Sparql.YasqeAjaxConfig): Promise<any>;
	getUrlParams(): queryString.ParsedQuery<string>;
	configToQueryParams(): queryString.ParsedQuery;
	queryParamsToConfig(params: queryString.ParsedQuery): void;
	getAsCurlString(config?: Sparql.YasqeAjaxConfig): string;
	abortQuery(): void;
	destroy(): void;
	static Sparql: typeof Sparql;
	static runMode: any;
	static clearStorage(): void;
	static Autocompleters: {
		[name: string]: Autocompleter.CompleterConfig;
	};
	static registerAutocompleter(value: Autocompleter.CompleterConfig, enable?: boolean): void;
	static defaults: {
		requestConfig: PlainRequestConfig;
		value?: any;
		mode: string;
		theme?: string;
		indentUnit?: number;
		smartIndent?: boolean;
		tabSize?: number;
		indentWithTabs?: boolean;
		electricChars?: boolean;
		rtlMoveVisually?: boolean;
		keyMap?: string;
		extraKeys?: string | import("codemirror").KeyMap;
		lineWrapping?: boolean;
		lineNumbers?: boolean;
		firstLineNumber?: number;
		lineNumberFormatter?: (line: number) => string;
		gutters?: string[];
		foldGutter: any;
		fixedGutter?: boolean;
		scrollbarStyle?: string;
		coverGutterNextToScrollbar?: boolean;
		inputStyle?: import("codemirror").InputStyle;
		readOnly?: any;
		screenReaderLabel?: string;
		showCursorWhenSelecting?: boolean;
		lineWiseCopyCut?: boolean;
		pasteLinesPerSelection?: boolean;
		selectionsMayTouch?: boolean;
		undoDepth?: number;
		historyEventDelay?: number;
		tabindex?: number;
		autofocus?: boolean;
		dragDrop?: boolean;
		allowDropFileTypes?: string[];
		onDragEvent?: (instance: import("codemirror").Editor, event: DragEvent) => boolean;
		onKeyEvent?: (instance: import("codemirror").Editor, event: KeyboardEvent) => boolean;
		cursorBlinkRate?: number;
		cursorScrollMargin?: number;
		cursorHeight?: number;
		resetSelectionOnContextMenu?: boolean;
		workTime?: number;
		workDelay?: number;
		pollInterval?: number;
		flattenSpans?: boolean;
		addModeClass?: boolean;
		maxHighlightLength?: number;
		viewportMargin?: number;
		spellcheck?: boolean;
		autocorrect?: boolean;
		autocapitalize?: boolean;
		lint?:
			| boolean
			| import("codemirror").LintStateOptions
			| import("codemirror").Linter
			| import("codemirror").AsyncLinter;
		collapsePrefixesOnLoad: boolean;
		syntaxErrorCheck: boolean;
		createShareableLink: (yasqe: Yasqe) => string;
		createShortLink: (yasqe: Yasqe, longLink: string) => Promise<string>;
		consumeShareLink: (yasqe: Yasqe) => void;
		persistenceId: string | ((yasqe: Yasqe) => string);
		persistencyExpire: number;
		showQueryButton: boolean;
		pluginButtons: () => HTMLElement | HTMLElement[];
		highlightSelectionMatches: {
			showToken?: RegExp;
			annotateScrollbar?: boolean;
		};
		tabMode: string;
		matchBrackets: boolean;
		autocompleters: string[];
		hintConfig: Partial<HintConfig>;
		resizeable: boolean;
		editorHeight: string;
		queryingDisabled: string;
	};
	static forkAutocompleter(
		fromCompleter: string,
		newCompleter: {
			name: string;
		} & Partial<Autocompleter.CompleterConfig>,
		enable?: boolean
	): void;

	// OWN EXTENSIONS
	setSize(a: any, b: any): void;
	setValue(s: string): void;
	getValue(): string;
	setOption(key: string, b: boolean);
}
export declare type TokenizerState = sparql11Mode.State;
export declare type Position = CodeMirror.Position;
export declare type Token = CodeMirror.Token;
export interface HintList {
	list: Hint[];
	from: Position;
	to: Position;
}
export interface Hint {
	text: string;
	displayText?: string;
	className?: string;
	render?: (el: HTMLElement, self: Hint, data: any) => void;
	from?: Position;
	to?: Position;
}
export declare type HintFn = {
	async?: boolean;
} & (() => Promise<HintList> | HintList);
export interface HintConfig {
	completeOnSingleClick?: boolean;
	container?: HTMLElement;
	closeCharacters?: RegExp;
	completeSingle?: boolean;
	hint: HintFn;
	alignWithWord?: boolean;
	closeOnUnfocus?: boolean;
	customKeys?: any;
	extraKeys?: {
		[key: string]: (
			yasqe: Yasqe,
			event: {
				close: () => void;
				data: {
					from: Position;
					to: Position;
					list: Hint[];
				};
				length: number;
				menuSize: () => void;
				moveFocus: (movement: number) => void;
				pick: () => void;
				setFocus: (index: number) => void;
			}
		) => void;
	};
}
export interface RequestConfig<Y> {
	queryArgument: string | ((yasqe: Y) => string) | undefined;
	endpoint: string | ((yasqe: Y) => string);
	method: "POST" | "GET" | ((yasqe: Y) => "POST" | "GET");
	acceptHeaderGraph: string | ((yasqe: Y) => string);
	acceptHeaderSelect: string | ((yasqe: Y) => string);
	acceptHeaderUpdate: string | ((yasqe: Y) => string);
	namedGraphs: string[] | ((yasqe: Y) => string[]);
	defaultGraphs: string[] | ((yasqe: Y) => []);
	args:
		| Array<{
				name: string;
				value: string;
		  }>
		| ((
				yasqe: Y
		  ) => Array<{
				name: string;
				value: string;
		  }>);
	headers:
		| {
				[key: string]: string;
		  }
		| ((
				yasqe: Y
		  ) => {
				[key: string]: string;
		  });
	withCredentials: boolean | ((yasqe: Y) => boolean);
	adjustQueryBeforeRequest: ((yasqe: Y) => string) | false;
}
export declare type PlainRequestConfig = {
	[K in keyof RequestConfig<any>]: Exclude<RequestConfig<any>[K], Function>;
};
export declare type PartialConfig = {
	[P in keyof Config]?: Config[P] extends object ? Partial<Config[P]> : Config[P];
};
export interface Config extends Partial<CodeMirror.EditorConfiguration> {
	mode: string;
	collapsePrefixesOnLoad: boolean;
	syntaxErrorCheck: boolean;
	createShareableLink: (yasqe: Yasqe) => string;
	createShortLink: ((yasqe: Yasqe, longLink: string) => Promise<string>) | undefined;
	consumeShareLink: ((yasqe: Yasqe) => void) | undefined | null;
	persistenceId: ((yasqe: Yasqe) => string) | string | undefined | null;
	persistencyExpire: number;
	showQueryButton: boolean;
	requestConfig: RequestConfig<Yasqe> | ((yasqe: Yasqe) => RequestConfig<Yasqe>);
	pluginButtons: (() => HTMLElement[] | HTMLElement) | undefined;
	highlightSelectionMatches: {
		showToken?: RegExp;
		annotateScrollbar?: boolean;
	};
	tabMode: string;
	foldGutter: any;
	matchBrackets: boolean;
	autocompleters: string[];
	hintConfig: Partial<HintConfig>;
	resizeable: boolean;
	editorHeight: string;
	queryingDisabled: string | undefined;

	// Own extension
	readOnly: boolean;
}
export interface PersistentConfig {
	query: string;
	editorHeight: string;
}

export default Yasqe;
