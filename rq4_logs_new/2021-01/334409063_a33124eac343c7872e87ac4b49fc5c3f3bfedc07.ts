import { AddEmployeeDispatch, DeleteEmployeeDispatch } from "./EmployeeAction";
import { AddProductDispatch, DeleteProductDispatch } from "./ProductActions";
import { AddDirectorDispatch, DeleteDirectorDispatch } from "./DirectorAction";

export enum ActionTypes{
  AddEmployee = "AddEmployee",
  DeleteEmployee = "DeleteEmployee",
  AddProduct = "AddProduct",
  DeleteProduct = "DeleteProduct",
  AddDirector = "AddDirector",
  DeleteDirector = "DeleteDirector"
}

export type EmployeeActions = AddEmployeeDispatch | DeleteEmployeeDispatch;
export type ProductActions = AddProductDispatch | DeleteProductDispatch;
export type DirectorActions = AddDirectorDispatch | DeleteDirectorDispatch;