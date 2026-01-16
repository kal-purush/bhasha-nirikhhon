import BaseFieldType from "./baseField";

export interface FieldType extends BaseFieldType {}

export const fields: FieldType[] = [
  {
    labelText: "Name",
    id: "name",
    name: "name",
    type: "text",
    isRequired: true,
  },
  {
    labelText: "Description",
    id: "description",
    name: "description",
    type: "text",
    isRequired: false,
  },
  {
    labelText: "Price",
    id: "price",
    name: "price",
    type: "number",
    isRequired: true,
  },
  {
    labelText: "Quantity",
    id: "quantity",
    name: "quantity",
    type: "number",
    isRequired: true,
  },
  {
    labelText: "Unit",
    id: "unit",
    name: "unit",
    type: "text",
    isRequired: true,
  },
];