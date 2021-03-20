export class InvalidExecutionPlanError extends Error {
	title: string;
	msg: string;
	date: Date;

	constructor(title: string, msg: string) {
		super(title + msg);
		this.name = "InvalidExecutionPlanError";
		this.title = title;
		this.msg = msg;
		this.date = new Date();
	}
}
