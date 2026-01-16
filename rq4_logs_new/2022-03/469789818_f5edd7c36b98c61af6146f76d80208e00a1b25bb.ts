const requirements = [0, 673, 689, 706, 723, 740, 757, 775, 793, 811, 829, 847];

export function calculateLevel(level: number): number {
  if (level < 12) {
    return requirements[level - 1];
  }

  return Math.floor(
    0.02 * Math.pow(level, 3) + 3.06 * Math.pow(level, 2) + 105.6 * level - 895
  );
}

export function calculateRange(
  currentLevel: number,
  desiredLevel: number
): number {
  if (currentLevel > desiredLevel) {
    return NaN;
  }

  if (currentLevel == desiredLevel) {
    return 0;
  }

  let runes = 0;
  for (let i = currentLevel; i < desiredLevel; i++) {
    runes += calculateLevel(i + 1);
  }

  return runes;
}