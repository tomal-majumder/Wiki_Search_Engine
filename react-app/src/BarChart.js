import React from 'react';
var CanvasJSReact = require('./canvasjs.react').default;
// eslint-disable-next-line
var CanvasJS = CanvasJSReact.CanvasJS;
var CanvasJSChart = CanvasJSReact.CanvasJSChart;

export class BarChart extends React.Component {
	render() {
		const optionsPython = {
			title: {
				text: "Basic Column Chart"
			},
			axisY: {
				title: "Post Count (10K)",
				tickLength: 10
			},
			data: [
				{
					// Change type to "doughnut", "line", "splineArea", etc.
					type: "column",
					dataPoints: [
						{ label: "2014", y: 11.6223 },
						{ label: "2015", y: 13.7057 },
						{ label: "2016", y: 15.8311 },
						{ label: "2017", y: 19.1872 },
						{ label: "2018", y: 20.6011 },
						{ label: "2019", y: 22.4827 },
						{ label: "2020", y: 29.9375 },
						{ label: "2021", y: 21.0389 }
					]
				}
			]
		}

		const optionsJavaScript = {
			title: {
				text: "Basic Column Chart"
			},
			axisY: {
				title: "Post Count (10K)",
				
			},
			data: [
				{
					// Change type to "doughnut", "line", "splineArea", etc.
					type: "column",
					dataPoints: [
						{ label: "2014", y: 23.5506 },
						{ label: "2015", y: 25.5483 },
						{ label: "2016", y: 26.3982 },
						{ label: "2017", y: 24.9896 },
						{ label: "2018", y: 20.8621 },
						{ label: "2019", y: 18.8940 },
						{ label: "2020", y: 22.4924 },
						{ label: "2021", y: 15.1374}
					]
				}
			]
		}

		const optionsJava = {
			title: {
				text: "Basic Column Chart"
			},
			axisY: {
				title: "Post Count (10K)",
				
			},
			data: [
				{
					// Change type to "doughnut", "line", "splineArea", etc.
					type: "column",
					dataPoints: [
						{ label: "2014", y: 21.7077 },
						{ label: "2015", y: 21.5208 },
						{ label: "2016", y: 19.6660 },
						{ label: "2017", y: 17.4335 },
						{ label: "2018", y: 14.5843 },
						{ label: "2019", y: 12.6700 },
						{ label: "2020", y: 12.7634 },
						{ label: "2021", y: 7.6436 }
					]
				}
			]
		}

		return (
			<div>
				<CanvasJSChart options={this.props.options}
				/* onRef={ref => this.chart = ref} */
				/>
				{/*You can get reference to the chart instance as shown above using onRef. This allows you to access all chart properties and methods*/}
			</div>
		);
	}
}