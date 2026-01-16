export interface Item {
  value: string;
  label: string;
}
export type SelectItems = Item[];

export const continentItems: SelectItems = [
  {
    value: "0",
    label: "All",
  },
  {
    value: "1",
    label: "Africa",
  },
  {
    value: "2",
    label: "Antartica",
  },
  {
    value: "3",
    label: "Asia",
  },
  {
    value: "4",
    label: "Europe",
  },
  {
    value: "5",
    label: "North America",
  },
  {
    value: "6",
    label: "Australia",
  },
  {
    value: "7",
    label: "South America",
  },
];