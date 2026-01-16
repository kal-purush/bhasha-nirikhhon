import React, { useState } from "react";
import { Bar } from "react-chartjs-2";
import "./BarGraphPrice.css";

import { colorsType, formatTimeData, sortData } from "./utils";

const BarGraphPrice = ({
  showHigherValue,
  showMinValue,
  showAveValue,
  maxValues,
  minValues,
  aveValues,
  timeValues,
}) => {
  let timeConstansts = formatTimeData(timeValues);
  let sortedPrices = sortData(maxValues);
  const maxPrice = sortedPrices[maxValues.length - 1];
  sortedPrices = sortData(minValues);
  const minPrice = sortedPrices[0];

  const [categoryPercentage] = useState(0.5);
  const [barPercentage] = useState(0.8);

  return (
    <div className="barGraphPrice">
      <Bar
        data={{
          labels: timeConstansts,
          datasets: [
            {
              categoryPercentage: categoryPercentage,
              barPercentage: barPercentage,
              label: "Max value",
              backgroundColor: colorsType.max,
              borderColor: colorsType.max,
              borderWidth: 1,
              hoverBackgroundColor: colorsType.maxHover,
              hoverBorderColor: colorsType.maxHover,
              data: showHigherValue && maxValues,
            },
            {
              categoryPercentage: categoryPercentage,
              barPercentage: barPercentage,

              label: "Average value",
              backgroundColor: colorsType.ave,
              borderColor: colorsType.ave,
              borderWidth: 1,
              hoverBackgroundColor: colorsType.aveHover,
              hoverBorderColor: colorsType.aveHover,
              data: showAveValue && aveValues,
            },
            {
              categoryPercentage: categoryPercentage,
              barPercentage: barPercentage,

              label: "Min value",
              backgroundColor: colorsType.min,
              borderColor: colorsType.min,
              borderWidth: 1,
              hoverBackgroundColor: colorsType.minHover,
              hoverBorderColor: colorsType.minHover,
              data: showMinValue && minValues,
            },
          ],
        }}
        options={{
          maintainAspectRatio: false,
          legend: {
            display: false,
          },
          scales: {
            yAxes: [
              {
                ticks: {
                  max: Math.round(maxPrice * 1.001),
                  min: Math.round(minPrice * 0.995),
                  beginAtZero: true,
                  stepSize: Math.round(
                    (Math.round(maxPrice * 1.001) -
                      Math.round(minPrice * 0.995)) /
                      3
                  ),
                },
              },
            ],
            xAxes: [
              {
                gridLines: {
                  display: false,
                },
              },
            ],
          },
        }}
      />
    </div>
  );
};

export default BarGraphPrice;