import * as dts from 'dts-element';
import * as R from 'ramda';
import {placeholder_name_abbr} from './constants';

export const ensure = <T>(value: any, is: (x: any) => x is T): T => {
  const is_something = is(value);
  // istanbul ignore next
  if (!is_something) {
    throw new Error('Invalid value');
  }
  return value;
};

export const is_element = (value: any): value is dts.IElement<dts.ElementKind> =>
  !R.isNil(value) && typeof value.kind === 'number';

export const is_type_predicate = (value: any): value is dts.ITypePredicate =>
  is_element(value) && (value.kind === dts.ElementKind.TypePredicate);

export const is_object_type = (value: any): value is dts.IObjectType =>
  is_element(value) && (value.kind === dts.ElementKind.ObjectType);

export const is_function_type = (value: any): value is dts.IFunctionType =>
  is_element(value) && (value.kind === dts.ElementKind.FunctionType);

export const is_object_member = (value: any): value is dts.IObjectMember =>
  is_element(value) && (value.kind === dts.ElementKind.ObjectMember);

export const is_function_declaration = (value: any): value is dts.IFunctionDeclaration =>
  is_element(value) && (value.kind === dts.ElementKind.FunctionDeclaration);

export const is_general_type = (value: any): value is dts.IGeneralType =>
  is_element(value) && (value.kind === dts.ElementKind.GeneralType);

export const is_placeholder = (value: any): value is dts.IGeneralType =>
  is_general_type(value) && (value.name === placeholder_name_abbr);