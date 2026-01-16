import moment from "moment/moment";
import ReactApexChart from "react-apexcharts";

import { data } from "../utils/const";

export const ApexChart = () => {
  const newData = data.map((item) => {
    return item.Date;
  });
  const newRevenue = data.map((item) => {
    return item.Revenue;
  });

  const apexData = {
    series: [
      {
        name: "Reveneu",
        data: newRevenue,
      },
    ],
    options: {
      chart: {
        id: "chart2",
        type: "line",
        height: 130,
        toolbar: {
          autoSelected: "pan",
          show: false,
        },
        brush: {
          enabled: false,
          autoScaleYaxis: true,
        },
      },
      colors: ["#546E7A"],
      stroke: {
        width: 3,
      },
      dataLabels: {
        enabled: false,
      },
      fill: {
        opacity: 1,
      },
      markers: {
        size: 0,
      },
      xaxis: {
        type: "datetime",
        categories: newData,
      },
    },

    seriesLine: [
      {
        data: newRevenue,
      },
    ],
    optionsLine: {
      chart: {
        id: "chart1",
        height: 130,
        type: "area",
        brush: {
          target: "chart2",
          enabled: true,
        },
        selection: {
          enabled: true,
          xaxis: {
            // min: new Date("19 Jun 2017").getTime(),
            // max: new Date("14 Aug 2017").getTime(),
            min: moment(newData[0]).format("MM/DD/YYYY"),
            max: new Date(
              moment(newData[newData.length - 1]).format("DD MMM YYYY")
            ).getTime(),
          },
        },
      },
      colors: ["#008FFB"],
      fill: {
        type: "gradient",
        gradient: {
          opacityFrom: 0.91,
          opacityTo: 0.1,
        },
      },
      xaxis: {
        type: "datetime",
        tooltip: {
          enabled: false,
        },
      },
      yaxis: {
        tickAmount: 2,
      },
    },
  };
  console.log(
    "data date",
    apexData,
    "time",
    {
      min: moment(newData).format("MM/DD/YYYY"),
      max: new Date(
        moment(newData[newData.length - 1]).format("DD MMM YYYY")
      ).getTime(),
    },
    newData
  );
  return (
    <div id="wrapper">
      <div id="chart-line2">
        <ReactApexChart
          options={apexData.options}
          series={apexData.series}
          type="line"
          height={280}
          width={1000}
        />
      </div>
      <div id="chart-line">
        <ReactApexChart
          options={apexData.optionsLine}
          type="area"
          series={apexData.seriesLine}
          height={280}
          width={1000}
        />
      </div>
    </div>
  );
};