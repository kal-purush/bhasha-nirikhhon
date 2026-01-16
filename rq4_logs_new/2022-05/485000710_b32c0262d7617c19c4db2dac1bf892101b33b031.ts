import { RefObject } from 'preact';
import { Toggle } from '../ui/toggel';
import { SidePanelItem } from './side-panel-item-dto';

/**
 * Panel item metadata
 * @internal
 */
export class ItemWrapper {
  private static nextId = 0;
  public readonly id: number;
  public readonly removePanelComponentFunc: () => void;
  public removeIconComponentFn: () => void;
  public readonly item: SidePanelItem;
  public readonly componentRef: RefObject<Toggle>;
  constructor(item: SidePanelItem, componentRef: RefObject<Toggle>, removePanelComponentFunc: () => void) {
    this.id = ++ItemWrapper.nextId;
    this.item = item;
    this.removePanelComponentFunc = removePanelComponentFunc;
    this.removeIconComponentFn = (): void => {
      return;
    };
    this.componentRef = componentRef;
  }

  set removeIconComponentFunc(removeIconComponentFunc: () => void) {
    this.removeIconComponentFn = removeIconComponentFunc;
  }
}