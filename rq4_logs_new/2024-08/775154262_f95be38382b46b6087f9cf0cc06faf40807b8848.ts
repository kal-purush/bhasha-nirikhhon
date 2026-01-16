import type { Meta, StoryObj } from '@storybook/react';
import BaseGraph from './BaseGraph';

export default {
  title: 'BaseGraph',
  component: BaseGraph,
  parameters: {
    actions: {
      handles: ['click .btn'],
    },
  },
} as Meta<typeof BaseGraph>;

type Story = StoryObj<typeof BaseGraph>;

export const Column: Story = {
  args: {
    animationEnabled: true,
    exportEnabled: true,
    theme: 'light2',
    text: 'Simple Column Chart with Index Labels',
    includeZero: true,
    type: 'column',
    indexLabelFontColor: '#5A5757',
    indexLabelPlacement: 'outside',
    dataPoints: [
      { x: 10, y: 71 },
      { x: 20, y: 55 },
      { x: 30, y: 50 },
      { x: 40, y: 65 },
      { x: 50, y: 71 },
      { x: 60, y: 68 },
      { x: 70, y: 38 },
      { x: 80, y: 92 },
      { x: 90, y: 54 },
      { x: 100, y: 60, indexLabel: 'high!' },
      { x: 110, y: 21 },
      { x: 120, y: 49 },
      { x: 130, y: 36 },
    ],
  },
};
export const Line: Story = {
  args: {
    animationEnabled: true,
    exportEnabled: true,
    theme: 'light2',
    text: 'Simple Column Chart with Index Labels',
    includeZero: true,
    type: 'spline',
    indexLabelFontColor: '#5A5757',
    indexLabelPlacement: 'outside',
    prefix: '$',
    title: 'Sales (in USD)',
    valueFormatString: 'MMM',
    xValueFormatString: 'MMMM',
    yValueFormatString: '$#,###',
    dataPoints: [
      { x: new Date(2017, 3), y: 32400 },
      { x: new Date(2017, 4), y: 35260 },
      { x: new Date(2017, 5), y: 33900 },
      { x: new Date(2017, 6), y: 40000 },
      { x: new Date(2017, 7), y: 52500, indexLabel: 'high!' },
      { x: new Date(2017, 8), y: 32300 },
      { x: new Date(2017, 9), y: 42000 },
      { x: new Date(2017, 10), y: 37160 },
      { x: new Date(2017, 11), y: 38400 },
    ],
  },
};
export const Pie: Story = {
  args: {
    animationEnabled: true,
    exportEnabled: true,
    theme: 'light1',
    text: 'Trip Expenses',
    type: 'pie',
    indexLabel: '{label}: {y}%',
    startAngle: -90,
    dataPoints: [
      { y: 20, label: 'Airfare' },
      { y: 24, label: 'Food & Drinks' },
      { y: 20, label: 'Accomodation' },
      { y: 14, label: 'Transportation' },
      { y: 12, label: 'Activities' },
      { y: 10, label: 'Misc' },
    ],
  },
};