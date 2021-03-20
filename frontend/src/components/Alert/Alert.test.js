// import React from "react";
// import Alert from "./Alert";
// import { AlertTitle, AlertDescription } from "@chakra-ui/react";

// import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
// import Enzyme, { shallow } from "enzyme";
// Enzyme.configure({ adapter: new Adapter() });

// it("Renders with valid status", () => {
// 	const alertTitle = "Alert Title";
// 	const alertDescription = "Alert description";
// 	const alertStatus = "error";
// 	const wrapper = shallow(
// 		<Alert title={alertTitle} description={alertDescription} status={alertStatus} />
// 	);

// 	expect(wrapper.find(AlertTitle).text()).toEqual(alertTitle);
// 	expect(wrapper.find(AlertDescription).text()).toEqual(alertDescription);
// });

// // it('No render due to invalid stauts', () => {
// //     const alertTitle = 'Unique Alert Title';
// //     const alertDescription = 'Alert description';
// //     const alertStatus = 'invalid status';
// //     render(<Alert title={alertTitle} description={alertDescription} status={alertStatus}/>);

// //     expect(screen.queryByText(alertTitle)).toBeNull();

// // });
