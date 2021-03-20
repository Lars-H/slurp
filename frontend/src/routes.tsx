import React, { Suspense, lazy } from "react";
import { Route, BrowserRouter, Switch } from "react-router-dom";
import Navbar from "./components/Navbar/Navbar";
import PageNotFound from "./pages/PageNotFound/PageNotFound";
import { Spinner } from "@chakra-ui/react";
import { ChakraProvider, Box, Center } from "@chakra-ui/react";

const MainPage = lazy(() => import("./pages/MainPage/MainPage"));
const CreateQueryPage = lazy(() => import("./pages/CreateQueryPage/CreateQueryPage"));
const TaskPage = lazy(() => import("./pages/TaskPage/TaskPage"));
const AboutPage = lazy(() => import("./pages/AboutPage/AboutPage"));
const VisualizerPage = lazy(() => import("./pages/VisualizerPage/VisualizerPage"));

export const createRoutes = () => {
	return (
		// <React.StrictMode>
		// TODO: ChakraProvider colorMode=light
		<ChakraProvider resetCSS={true}>
			{/* TODO: basename Klären */}
			<BrowserRouter>
				<Navbar />
				<Center>
					<Box mb={4} w="80%" mx={5} mt={4}>
						<Suspense fallback={<Spinner />}>
							<Switch>
								<Route exact path="/" component={MainPage} />
								<Route exact path="/new" component={CreateQueryPage} />
								<Route exact path="/task/:taskId" component={TaskPage} />
								<Route exact path="/visualizer" component={VisualizerPage} />
								<Route exact path="/about" component={AboutPage} />
								<Route component={PageNotFound} />
							</Switch>
						</Suspense>
					</Box>
				</Center>
			</BrowserRouter>
		</ChakraProvider>
		// </React.StrictMode>
	);
};
