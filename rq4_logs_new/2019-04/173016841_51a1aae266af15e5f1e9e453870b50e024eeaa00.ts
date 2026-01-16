import { useState, useEffect } from 'react';
import { getRandomItem, randomWholeNumber } from '../../util';

type Range = [number, number];

interface UseConfettiParticlesStylesSettings {
  sizeRange: Range;
  rotationRange: Range;
  leftRange: Range;
  colorOptions?: string[];
  topOffset: number;
}

const DEFAULT_COLOR_OPTIONS = [
  '#2ecc71',
  '#3498db',
  '#e67e22',
  '#e67e22',
  '#e74c3c'
];

function useConfettiParticleStyles({
  sizeRange,
  rotationRange,
  leftRange,
  colorOptions = DEFAULT_COLOR_OPTIONS,
  topOffset
}: UseConfettiParticlesStylesSettings) {
  const [color] = useState(() => getRandomItem(colorOptions));
  const [size] = useState(() => randomWholeNumber(...sizeRange));
  const [rotation] = useState(() => randomWholeNumber(...rotationRange));
  const [left, setLeft] = useState(() =>
    randomWholeNumber(0, window.innerWidth)
  );
  const [top, setTop] = useState(() => randomWholeNumber(-topOffset, 0));

  useEffect(() => {
    setLeft(left => left + randomWholeNumber(...leftRange));
    setTop(topOffset + randomWholeNumber(0, topOffset));
  }, []);

  return {
    top,
    left,
    rotation: `rotateZ(${rotation}deg)`,
    size,
    color,
    position: 'absolute' as 'absolute',
    transition: 'all 5s ease-out'
  };
}

function useConfettiParticleStylesDiv(
  settings: UseConfettiParticlesStylesSettings
) {
  const { size, color, ...rest } = useConfettiParticleStyles(settings);
  return {
    width: size,
    height: size,
    borderRadius: size,
    backgroundColor: color,
    ...rest
  };
}

function useConfettiParticleStylesSVG(
  settings: UseConfettiParticlesStylesSettings
) {
  const { size, color, ...rest } = useConfettiParticleStyles(settings);
  return {
    width: size,
    height: size,
    fill: color,
    ...rest
  };
}

export default {
  div: useConfettiParticleStylesDiv,
  svg: useConfettiParticleStylesSVG
};