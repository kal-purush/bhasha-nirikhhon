import * as RadioGroupPrimitive from '@radix-ui/react-radio-group';
import * as React from 'react';

export type RadioGroupRef = React.ComponentRef<typeof RadioGroupPrimitive.Root>;
export type RadioGroupProps = React.ComponentPropsWithoutRef<
  typeof RadioGroupPrimitive.Root
>;

export type RadioGroupItemRef = React.ComponentRef<
  typeof RadioGroupPrimitive.Item
>;
export type RadioGroupItemProps = React.ComponentPropsWithoutRef<
  typeof RadioGroupPrimitive.Item
>;

export type RadioGroupComponent = typeof RadioGroupPrimitive.Root & {
  Item: typeof RadioGroupPrimitive.Item;
};