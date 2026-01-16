import * as ScrollAreaPrimitive from '@radix-ui/react-scroll-area';
import * as React from 'react';

export type ScrollAreaRef = React.ComponentRef<typeof ScrollAreaPrimitive.Root>;
export type ScrollAreaProps = React.ComponentPropsWithoutRef<
  typeof ScrollAreaPrimitive.Root
>;

export type ScrollBarRef = React.ComponentRef<
  typeof ScrollAreaPrimitive.ScrollAreaScrollbar
>;
export type ScrollBarProps = React.ComponentPropsWithoutRef<
  typeof ScrollAreaPrimitive.ScrollAreaScrollbar
>;

export type ScrollBarThumbRef = React.ComponentRef<
  typeof ScrollAreaPrimitive.ScrollAreaThumb
>;
export type ScrollBarThumbProps = React.ComponentPropsWithoutRef<
  typeof ScrollAreaPrimitive.ScrollAreaThumb
>;

export type ScrollAreaComponent = typeof ScrollAreaPrimitive.Root & {
  ScrollBar: typeof ScrollAreaPrimitive.ScrollAreaScrollbar;
  ScrollBarThumb: typeof ScrollAreaPrimitive.ScrollAreaThumb;
};