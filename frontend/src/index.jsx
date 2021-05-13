import ReactDOM from "react-dom";
import "./index.css";
import reportWebVitals from "./reportWebVitals";
import { createRoutes } from "./routes";

const render = () => {
	ReactDOM.render(createRoutes(), document.getElementById("root"));
};

if (module.hot) {
	module.hot.accept("./", () => {
		setTimeout(render);
	});
}

render();

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
