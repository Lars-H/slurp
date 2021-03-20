import React from "react";
// import "./Navbar.scss";
import logo from "./logo.png";
import { withRouter, Link } from "react-router-dom";
import { Box, Flex, Text, HStack } from "@chakra-ui/react";
import DarkModeToggle from "./DarkModeToggle/DarkModeToggle";

const Navbar = () => {
	return (
		// <div className="navbar">
		//     <div className="navbar-brand">
		//         Query Planner
		//     </div>
		//     {createNavItem()}
		// </div>
		<Flex
			className="navbar"
			w="100%"
			px={5}
			py={4}
			height="64px"
			borderColor="transparent"
			backgroundImage="linear-gradient(to right, #2d3c4d, #2a3848, #263442, #23303d, #202c38);"
			justifyContent="space-between"
			alignItems="center"
			mb="3"
			textDecoration="none"
		>
			<Link to="/">
				<HStack justifyContent="center" alignItems="center">
					<img
						style={{ marginRight: "5px" }}
						src={logo}
						width="35"
						alt="SLURP logo"
					></img>
					<Box>
						<Text
							color="white"
							fontWeight="bold"
							className="navbar-brand"
							fontSize="21px"
							mt="-1"
						>
							SLURP
						</Text>
						<Text
							color="white"
							className="navbar-brand-secondary"
							mt="-2"
							fontSize="13px"
						>
							<b>S</b>PARQ<b>L</b> Q<b>u</b>e<b>r</b>y <b>P</b>lanner
						</Text>
					</Box>
				</HStack>
			</Link>

			<HStack position="absolute" right="5" ml="50">
				{process.env.NODE_ENV !== "production" && (
					<Link to="/visualizer">
						<Text color="white" mr="30">
							Visualizer
						</Text>
					</Link>
				)}
				<Link to="/about">
					<Text color="white" mr="30">
						About
					</Text>
				</Link>
				<DarkModeToggle />
			</HStack>
		</Flex>
	);
};

export default withRouter(Navbar);
