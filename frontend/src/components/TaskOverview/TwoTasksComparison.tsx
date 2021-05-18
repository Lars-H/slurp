import {ExpandedIndex} from "@chakra-ui/accordion";
import {Box} from "@chakra-ui/react";
import {ITaskPageDataResponse} from "interface/ITaskPageDataResponse";
import {useState} from "react";
import TaskOverview from "./TaskOverview";

interface TwoTasksComparisonProps {
    first: ITaskPageDataResponse;
    second: ITaskPageDataResponse;
}

const TwoTasksComparison = (props: TwoTasksComparisonProps) => {
    const [extendedItems, setExtendedItems] = useState<ExpandedIndex>([]);

    return (
        <>
            <TaskOverview
                {... props.first}
                extendedItems={extendedItems}
                updateExtendedItems={setExtendedItems}
            />
            <Box marginLeft="32px"></Box>
            <TaskOverview
                {... props.second}
                extendedItems={extendedItems}
                updateExtendedItems={setExtendedItems}
            />
        </>
    )
}

export default TwoTasksComparison;