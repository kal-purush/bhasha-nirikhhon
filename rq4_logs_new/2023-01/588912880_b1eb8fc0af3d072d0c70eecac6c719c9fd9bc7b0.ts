export const dailyGoalCalc = (weight: string) => {
  const water = 35;
  const n = parseInt(weight);
  const unit = weight.substring(-2);
  if (unit === 'lb') {
    const ratio = 2.205;
    const lbToKg = n / ratio;
    const result = lbToKg * water;
    return Math.round(result);
  }

  const result = n * water;
  return Math.round(result);
};