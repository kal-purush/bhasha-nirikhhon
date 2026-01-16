/**
 * Calculate the lerp weight between two points by comparing each points value.
 *
 * @export
 * @param {number} valueA
 * @param {number} valueB
 * @returns {number}
 */
export function lerpWeight(valueA: number, valueB: number): number {
  const weight: number = 1 - (valueA + valueB + 2) / 4; // Weighted inverted
  // const lerpWeight: number = (valueA + valueB + 2) / 4; // Weighted
  // const lerpWeight = 0.5; // Centered
  return weight;
}

/**
 * Calculate the marching square segment based on the values of the four points. The values passed to this function should be either 0 or 1;
 *
 * @export
 * @param {number} pointA
 * @param {number} pointB
 * @param {number} pointC
 * @param {number} pointD
 * @returns {number}
 */
export function calculateSegment(pointA: number, pointB: number, pointC: number, pointD: number): number {
  return pointA * 1 + pointB * 2 + pointC * 4 + pointD * 8;
}