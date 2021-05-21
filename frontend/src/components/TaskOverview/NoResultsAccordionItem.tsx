import {AccordionItem, Box} from "@chakra-ui/react"

export const NoResultsAccordionItem = () => {
    return (
        <AccordionItem>
            <Box
                height="40px"
                borderBottomColor="rgb(226, 232, 240)"
                padding="8px 16px"
                userSelect="none"
            >
                No Results
			</Box>
        </AccordionItem>
    );
}