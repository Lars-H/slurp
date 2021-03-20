import React from "react";
import { useColorMode, IconButton } from "@chakra-ui/react";
import { MoonIcon, SunIcon } from "@chakra-ui/icons";

function DarkModeToggle() {
	const { colorMode, toggleColorMode } = useColorMode();
	return (
		<>
			{colorMode === "light" ? (
				<IconButton
					onClick={toggleColorMode}
					aria-label="Toggle Dark Mode"
					icon={<MoonIcon />}
				/>
			) : (
				<IconButton
					onClick={toggleColorMode}
					aria-label="Toggle Light Mode"
					icon={<SunIcon />}
				/>
			)}
		</>
	);
}

export default DarkModeToggle;
