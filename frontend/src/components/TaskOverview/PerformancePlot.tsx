import { Box } from "@chakra-ui/react";
import React, { useEffect, useState } from "react";

import Highcharts, { Chart, Options, TooltipFormatterContextObject } from "highcharts";
import HighchartsReact from "highcharts-react-official";
import NoDataToDisplay from "highcharts/modules/no-data-to-display";

NoDataToDisplay(Highcharts);

export type OverviewElements = "heading" | "info" | "query" | "plan" | "results";

interface PerformancePlotProps {
	data: [number, number][];
	plotLimits?: {x: number, y: number};
}

const PerformancePlot = (props: PerformancePlotProps) => {
	const [chartOptions, setChartOptions] = useState<Options>({
		chart: {
			type: "scatter",
		},
		series: [
			{
				data: [],
				type: "scatter",
			},
		],
		yAxis: {
			title: {
				text: "# results produced",
				style: {
					fontSize: "14px",
				},
				margin: 16,
			},
			max: props.plotLimits ? props.plotLimits.y : null
		},
		xAxis: {
			title: {
				text: "time in seconds",
				style: {
					fontSize: "14px",
				},
				margin: 12,
			},
			min: 0,
			max: props.plotLimits ? props.plotLimits.x : null,
		},
		legend: {
			enabled: false,
		},
		title: {
			text: "",
		},
		lang: {
			noData: "No data available",
		},
		credits: {
			enabled: false,
		},
		tooltip: {
            useHTML: true,
			formatter: function () {
				return `<table>
                    <tr>
                        <td><b>results:</b></td>
                        <td style="padding-left: 5px">${this.point.y}</td>
                    </tr>
                    <tr>
                        <td><b>time:</b></td>
                        <td style="padding-left: 5px">${(Math.round(this.point.x * 10) / 10).toFixed(1)} sec</td>
                    </tr>
                </table>`;
			},
		},
	});

	useEffect(() => {
		setChartOptions({
			series: [
				{
					data: props.data,
					type: "scatter",
				},
			],
		});
	}, [props.data]);

	return (
		<Box mt="20px">
			<HighchartsReact highcharts={Highcharts} options={chartOptions} />
		</Box>
	);
};

export default PerformancePlot;
