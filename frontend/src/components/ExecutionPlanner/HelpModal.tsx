import React from "react";

import {
	Button,
	useDisclosure,
	Modal,
	ModalOverlay,
	ModalContent,
	ModalHeader,
	ModalBody,
	ModalCloseButton,
	ModalFooter,
	Text,
	Center,
	Accordion,
	AccordionItem,
	AccordionPanel,
	AccordionButton,
	AccordionIcon,
	Box,
} from "@chakra-ui/react";

const HelpModal: React.FC = () => {
	const { isOpen, onOpen, onClose } = useDisclosure();
	return (
		<>
			<Button size="xs" onClick={onOpen}>
				?
			</Button>

			<Modal size="xl" isOpen={isOpen} onClose={onClose}>
				<ModalOverlay />
				<ModalContent>
					<ModalHeader>Customizing the query execution plan</ModalHeader>
					<ModalCloseButton />
					<ModalBody>
						<Text mb="5">
							The tree consists of triple patterns (leaves indicated by their
							respective line number in the query editor) and joins (nodes indicated
							by their jointype). There are several ways to modify the execution plan:
						</Text>
						<Accordion>
							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Change Join Type
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<Text>
										Right click on any join node to change its join algorithm.
									</Text>
									<Center>
										<img
											src="./gif/changejointype.gif"
											width="250"
											alt="Change node join type"
										/>
									</Center>
								</AccordionPanel>
							</AccordionItem>

							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Exchange triple patterns
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<Text>
										Double click any triple pattern to select it. Click another
										triple pattern to swap places.
									</Text>
									<Center>
										<img
											src="./gif/switchleaves.gif"
											width="250"
											alt="Swap triple patterns"
										/>
									</Center>
								</AccordionPanel>
							</AccordionItem>
							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Build plan from scratch, undo and reset to default
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<Text>
										Click and hold anywhere outside the tree to build a plan
										from scratch, undo the last action or reset to the suggested
										plan by the query engine.
									</Text>
									<Center>
										<img
											src="./gif/startfromscratch.gif"
											width="400"
											alt="Start plan from scratch"
										/>
									</Center>
								</AccordionPanel>
							</AccordionItem>
							<AccordionItem>
								<AccordionButton>
									<Box flex="1" textAlign="left">
										Join Leaves and Subtrees
									</Box>
									<AccordionIcon />
								</AccordionButton>
								<AccordionPanel pb={4}>
									<Text>
										Click and hold a leaf or subtree and drop it onto another
										leaf or subtree to join them.
									</Text>
									<Center>
										<img
											src="./gif/draganddrop.gif"
											width="400"
											alt="Swap triple patterns"
										/>
									</Center>
								</AccordionPanel>
							</AccordionItem>
						</Accordion>
					</ModalBody>

					<ModalFooter>
						<Button colorScheme="blue" mr={3} onClick={onClose}>
							Close
						</Button>
					</ModalFooter>
				</ModalContent>
			</Modal>
		</>
	);
};

export default HelpModal;
