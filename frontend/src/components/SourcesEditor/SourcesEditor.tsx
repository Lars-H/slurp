import React, { useState } from "react";
import { InputGroup, InputLeftAddon, Input, Badge, HStack, IconButton } from "@chakra-ui/react";
import { CloseIcon, AddIcon } from "@chakra-ui/icons";

interface ISourceEditorProps {
	sources: Set<string>;
	setSources: React.Dispatch<React.SetStateAction<Set<string>>>;
	querySubmitted: boolean;
}

function SourcesEditor(props: ISourceEditorProps) {
	const [validSource, setValidSource] = useState(true);
	const [newSource, setNewSource] = useState("");

	const addSource = () => {
		if (isSourceValid(newSource)) {
			const newSources = new Set(props.sources);
			// Discovered that backend throws an exception if a URL with CAPS is provided. Therefore the sources are served in lowercase letters.
			newSources.add(newSource.toLowerCase());
			localStorage.setItem("sources", JSON.stringify([...newSources]));
			props.setSources(newSources);
		}
	};

	const removeSource = (source) => {
		const sourceToRemove = source;
		const newSources = new Set(props.sources);
		newSources.delete(sourceToRemove);
		props.setSources(newSources);
		localStorage.setItem("sources", JSON.stringify([...newSources]));
	};

	const handleSourceInputChange = (evt) => {
		const newValue = evt.target.value;
		setNewSource(newValue);

		if (newSource.length > 0) {
			setValidSource(isSourceValid(newValue));
		}
	};

	const isSourceValid = (sourceToVerify) => {
		return /(tpf@)?(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})/i.test(
			sourceToVerify
		);
	};

	const handleKeyDown = (event) => {
		if (event.key === "Enter" && !props.querySubmitted) {
			addSource();
		}
	};

	return (
		<>
			<HStack align="stretch" mb="4">
				<InputGroup>
					<InputLeftAddon borderRadius={0}>Source</InputLeftAddon>
					<Input
						borderRadius={0}
						type="text"
						placeholder="TPF Source URL"
						onChange={handleSourceInputChange}
						errorBorderColor="red.300"
						isInvalid={!validSource}
						onKeyDown={handleKeyDown}
					/>
				</InputGroup>
				<IconButton
					disabled={!validSource || newSource.length === 0 || props.querySubmitted}
					size="md"
					ml="2"
					p="4"
					width="200px"
					onClick={addSource}
					aria-label="add source"
					icon={<AddIcon />}
				></IconButton>
			</HStack>
			{[...props.sources].map((el) => {
				return (
					<Badge mb="4" mr="4" p="2" key={el}>
						{el}
						<IconButton
							disabled={props.querySubmitted}
							ml="2"
							size="xs"
							onClick={() => removeSource(el)}
							aria-label="remove source"
							icon={<CloseIcon />}
						/>
					</Badge>
				);
			})}
		</>
	);
}

export default SourcesEditor;
