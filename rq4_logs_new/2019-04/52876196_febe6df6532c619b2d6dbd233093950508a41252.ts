import { InjectionToken } from '@angular/core';
import { Placement } from '../util/overlay-position';

/** Injection token that can be used to specify default options. */
export const NGL_TOOLTIP_CONFIG = new InjectionToken<NglTooltipConfig>('ngl-tooltip-config');

export class NglTooltipConfig<D = any> {

  /**
   * Default position relative to host element.
   */
  placement: Placement = 'top';

  /**
   * Whether you can interact with the content of the tooltip.
   */
  interactive = false;

  /**
   * Whether tooltip will open/close without two-way binding input.
   */
  openAuto = false;
}