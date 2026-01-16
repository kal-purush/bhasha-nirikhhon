import * as React from 'react';
import * as ResizablePrimitive from 'react-resizable-panels';

export type ResizableGroupProps = React.ComponentProps<
  typeof ResizablePrimitive.PanelGroup
>;

export type ResizableHandleProps = React.ComponentProps<
  typeof ResizablePrimitive.PanelResizeHandle
> & {
  withHandle?: boolean;
};

export type ResizableGroupComponent =
  React.FunctionComponent<ResizableGroupProps> & {
    Panel: typeof ResizablePrimitive.Panel;
    Handle: React.FunctionComponent<ResizableHandleProps> & {
      displayName?: string;
    };
  };