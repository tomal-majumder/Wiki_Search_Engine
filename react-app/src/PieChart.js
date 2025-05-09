import React from 'react';
var CanvasJSReact = require('./canvasjs.react').default;
// eslint-disable-next-line
var CanvasJS = CanvasJSReact.CanvasJS;
var CanvasJSChart = CanvasJSReact.CanvasJSChart;

export class PieChart extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            exportEnabled: true,
            animationEnabled: true,
            titleText:  "StackOverFlow Data Analysis",
            type: "pie",
            startAngle: 75,
            indexLabelFontSize: 16,
            dataPoints: [
                { y: 18, label: "Direct" },
                { y: 49, label: "Organic Search" },
                { y: 9, label: "Paid Search" },
                { y: 5, label: "Referral" },
                { y: 19, label: "Social" }
            ]
        };
    }

    render() {
        const options = {
            exportEnabled: true,
            animationEnabled: true,
            title: {
                text: "StackOverFlow Data Analysis"
            },
            data: [{
                type: "pie",
                startAngle: 75,
                toolTipContent: "<b>{label}</b>: {y}%",
                showInLegend: "true",
                legendText: "{label}",
                indexLabelFontSize: 16,
                indexLabel: "{label} - {y}%",
                dataPoints: [
                    { y: 18, label: "Direct" },
                    { y: 49, label: "Organic Search" },
                    { y: 9, label: "Paid Search" },
                    { y: 5, label: "Referral" },
                    { y: 19, label: "Social" }
                ]
            }]
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