import hslToRgb from "./hslToRgb";
import expandColor from "./expandColor";

function rand(min: number, max: number) {
  return Math.random() * (max - min) + min;
}

export default function generateRandomNiceColor() {
  const rgb = hslToRgb(rand(0, 360), rand(0.25, 1), rand(0.3, 0.6));
  return expandColor(rgb);
}