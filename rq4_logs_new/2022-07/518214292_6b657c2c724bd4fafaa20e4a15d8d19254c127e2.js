export const BRANDS = [
  {
    id: 1,
    name: "Europeo",
  },
  {
    id: 2,
    name: "Americano",
  },
  {
    id: 3,
    name: "Asiático",
  },
];

const YEARMAX = new Date().getFullYear();
export const YEARS = Array.from(
  new Array(20),
  (value, index) => YEARMAX - index
);

export const PLANS = [
  {
    id: 1,
    name: "Básico",
  },
  {
    id: 2,
    name: "Completo",
  },
];