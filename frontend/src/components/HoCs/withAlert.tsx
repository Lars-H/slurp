import Alert, { AlertStatus } from "components/Alert/Alert";
import React, { useState } from "react";

export interface IAlertProps {
	setAlert: React.Dispatch<React.SetStateAction<AlertContent | null>>;
}

export interface AlertContent {
	msg: string;
	title: string;
	status: AlertStatus;
}

export const renderAlert = (alertObj: AlertContent | null) => {
	if (alertObj && alertObj.title && alertObj.msg && alertObj.status) {
		return <Alert title={alertObj.title} description={alertObj.msg} status={alertObj.status} />;
	}
	return null;
};

const withAlert = <P extends IAlertProps>(
	WrappedComponent: React.ComponentType<P>
): React.FunctionComponent<P> => {
	return (props) => {
		const [alert, setAlert] = useState<AlertContent | null>(null);
		// TODO: scroll to view
		// const myRef = useRef(null);

		return (
			<>
				{renderAlert(alert)}
				<WrappedComponent {...props} setAlert={setAlert} />
			</>
		);
	};
};

export default withAlert;
