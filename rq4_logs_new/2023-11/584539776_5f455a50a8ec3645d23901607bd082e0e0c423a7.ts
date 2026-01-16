import { modalScaleDefinitions } from '../../consts/scales.js';
import { Scale } from './getScale.js';

export const getParallelModeNames = (scale: Scale) => {
  if (scale.modeOf === undefined) return undefined;

  return modalScaleDefinitions[scale.modeOf].map(
    (mode) => `${scale.root} ${mode.name}`
  );
};