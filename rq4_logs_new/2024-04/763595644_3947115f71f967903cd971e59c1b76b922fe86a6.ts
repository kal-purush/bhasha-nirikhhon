import type {
  ComponentProps,
  ComponentType,
  MutableRefObject,
  ReactNode,
} from 'react';

type ToggleEventHandler = (open: boolean) => void;

export type PortalContainerEl = HTMLElement | undefined | null;

export interface TogglePortalContextValue {
  containerEl?: HTMLElement;
  open: boolean;
  toggleRef?: MutableRefObject<ToggleEventHandler>;
}

export interface TogglePortalProviderProps
  extends Pick<TogglePortalContextValue, 'open'> {
  render: (containerRef: (el: PortalContainerEl) => void) => ReactNode;
  onToggle: ToggleEventHandler;
}

export type PortalWrapperProps<W extends ComponentType<any>> =
  ComponentProps<W> & {
    children: ReactNode;
    containerEl?: PortalContainerEl;
    WrapperComponent?: W;
  };