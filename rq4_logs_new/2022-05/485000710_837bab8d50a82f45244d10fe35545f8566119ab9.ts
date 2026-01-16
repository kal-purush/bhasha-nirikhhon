import { ui, KalturaPlayer, Logger, PlaykitUI } from 'kaltura-player-js';
import { SidePanelItem } from './models/side-panel-item-dto';
import { ItemWrapper } from './models/item-wrapper';
const { SidePanelModes, SidePanelPositions, ReservedPresetNames } = ui;

const OPPOSITE_PANELS: Record<PlaykitUI.SidePanelPosition, PlaykitUI.SidePanelPosition> = {
  [SidePanelPositions.TOP]: SidePanelPositions.BOTTOM,
  [SidePanelPositions.BOTTOM]: SidePanelPositions.TOP,
  [SidePanelPositions.RIGHT]: SidePanelPositions.LEFT,
  [SidePanelPositions.LEFT]: SidePanelPositions.RIGHT
} as Record<PlaykitUI.SidePanelPosition, PlaykitUI.SidePanelPosition>;

export class SidePanelsManager {
  private readonly player: KalturaPlayer;
  private readonly activePanels: Record<PlaykitUI.SidePanelPosition, ItemWrapper | null>;
  private readonly componentsRegistry: Map<number, ItemWrapper>;
  private readonly logger: Logger;

  constructor(player: KalturaPlayer, logger: Logger) {
    this.player = player;
    this.activePanels = { top: null, bottom: null, right: null, left: null };
    this.componentsRegistry = new Map<number, ItemWrapper>();
    this.logger = logger;
  }

  public addItem(item: SidePanelItem): number | void {
    if (SidePanelsManager.validateItem(item)) {
      const newItemWrapper: ItemWrapper = new ItemWrapper(item, this.player, (id: number) => this.toggle(id));
      this.componentsRegistry.set(newItemWrapper.id, newItemWrapper);
      this.logger.debug('New Panel Item Added', item);
      return newItemWrapper.id;
    }
    this.logger.warn('Invalid SidePanelItem parameters', item);
  }

  public removeItem(itemId: number): void {
    const itemWrapper: ItemWrapper | undefined = this.componentsRegistry.get(itemId);
    if (itemWrapper) {
      if (this.isItemActive(itemId)) this.deactivateItem(itemId);
      itemWrapper.remove();
      this.componentsRegistry.delete(itemId);
    } else {
      this.logger.warn(`${itemId} is not registered`);
    }
  }

  public activateItem(itemId: number): void {
    const itemWrapper: ItemWrapper | undefined = this.componentsRegistry.get(itemId);
    if (itemWrapper) {
      const { position, expandMode } = itemWrapper.item;
      // Trying to activate an already active item
      if (this.isItemActive(itemId)) return;
      // Switch between items if currently there is an active one (without collapsing / expanding PS)
      const previousItemWrapper = this.activePanels[position];
      if (previousItemWrapper !== null) {
        previousItemWrapper.deactivate(true);
      }
      // Deactivate the opposite panel if is active
      const oppositePosition: PlaykitUI.SidePanelPosition = SidePanelsManager.getOppositePanelPosition(position);
      if (this.activePanels[oppositePosition]) {
        this.deactivateItem(this.activePanels[oppositePosition]!.id);
      }
      // Update new item as active
      itemWrapper.activate();
      this.expand(position, expandMode);
      this.activePanels[position] = itemWrapper;
    } else {
      this.logger.warn(`${itemId} is not registered`);
    }
  }

  public deactivateItem(itemId: number): void {
    const itemWrapper: ItemWrapper | undefined = this.componentsRegistry.get(itemId);
    if (itemWrapper) {
      if (!this.isItemActive(itemId)) return;
      const { position } = itemWrapper.item;
      itemWrapper.deactivate();
      this.collapse(position);
      this.activePanels[position] = null;
    } else {
      this.logger.warn(`${itemId} is not registered`);
    }
  }

  public isItemActive(itemId: number): boolean {
    const itemWrapper: ItemWrapper | undefined = this.componentsRegistry.get(itemId);
    if (itemWrapper) {
      return this.activePanels[itemWrapper.item.position]?.id === itemId;
    }
    this.logger.warn(`${itemId} is not registered`);
    return false;
  }

  public update(itemId: number): void {
    const itemWrapper: ItemWrapper | undefined = this.componentsRegistry.get(itemId);
    if (itemWrapper) {
      itemWrapper.update();
    } else {
      this.logger.warn(`${itemId} is not registered`);
    }
  }

  public reset(): void {
    for (const value of this.componentsRegistry.values()) {
      this.removeItem(value.id);
    }
  }

  private toggle(itemId: number): void {
    if (this.isItemActive(itemId)) {
      this.deactivateItem(itemId);
    } else {
      this.activateItem(itemId);
    }
  }

  private expand(position: PlaykitUI.SidePanelPosition, expandMode: PlaykitUI.SidePanelMode): void {
    this.player.ui.store.dispatch(ui.reducers.shell.actions.updateSidePanelMode(position, expandMode));
  }

  private collapse(position: PlaykitUI.SidePanelPosition): void {
    this.player.ui.store.dispatch(ui.reducers.shell.actions.updateSidePanelMode(position, SidePanelModes.HIDDEN));
  }

  private static getOppositePanelPosition(position: PlaykitUI.SidePanelPosition): PlaykitUI.SidePanelPosition {
    return OPPOSITE_PANELS[position];
  }

  private static validateItem(item: SidePanelItem): boolean {
    const { label, panelComponent, iconComponent, position, expandMode, onActivate, onDeactivate, presets } = item;
    return !!(
      label &&
      Object.values(SidePanelPositions).includes(position) &&
      Object.values(SidePanelModes).includes(expandMode) &&
      presets.every((preset) => Object.values(ReservedPresetNames).includes(preset)) &&
      typeof panelComponent === 'function' &&
      (typeof iconComponent === 'function' || iconComponent === undefined) &&
      (typeof onActivate === 'function' || onActivate === undefined) &&
      (typeof onDeactivate === 'function' || onDeactivate === undefined)
    );
  }
}