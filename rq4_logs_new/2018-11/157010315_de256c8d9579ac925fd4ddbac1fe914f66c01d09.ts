export const COLOR_MAP = [
  { name: "red", value: "#f00" },
  { name: "orange", value: "#f70" },
  { name: "yellow", value: "#ff0" },
  { name: "chartreuse", value: "#7f0" },
  { name: "green", value: "#0f0" },
  { name: "spring green", value: "#0f7" },
  { name: "cyan", value: "#0ff" },
  { name: "azure", value: "#07f" },
  { name: "blue", value: "#00f" },
  { name: "violet", value: "#70f" },
  { name: "magenta", value: "#f0f" },
  { name: "rose", value: "#f07" }
];
const WHITE = { name: "WHITE", value: "#fff" };
const BLACK = { name: "black", value: "000" };

export function parseHexColor(hexColor: string, simplified = false) {
  if (hexColor.startsWith("#")) {
    hexColor = hexColor.slice(1);
  }

  switch (hexColor.length) {
    case 3:
      return Array.from(hexColor).map(char => parseInt(char.repeat(2), 16));
    case 6:
      return chunk(Array.from(hexColor), 2).map(arr => {
        const numStr = simplified ? arr[0].repeat(2) : arr.join("");
        return parseInt(numStr, 16);
      });
    default:
      return [0, 0, 0];
  }
}

export function asPercentage(rgbComponent: number) {
  return `${(rgbComponent / 255) * 100}%`;
}

export function mapToHue(hexColor: string) {
  const colorComponents = parseHexColor(hexColor, true);

  if (colorComponents.every(c => c === 255)) {
    return WHITE;
  } else if (colorComponents.every(c => c === 0)) {
    return BLACK;
  } else if (colorComponents.every(c => c === colorComponents[0])) {
    return {
      name: "grey",
      value: colorComponents.map(c => c.toString(16)).join()
    };
  }

  const errors = COLOR_MAP.map(color => {
    const parsedColor = parseHexColor(color.value);
    return colorComponents.reduce((sum, c, idx) => sum + (c - parsedColor[idx]) ** 2, 0) / colorComponents.length;
  });

  const indexOfMin = COLOR_MAP.reduce((res, current, idx) => {
    return errors[idx] < errors[res] ? idx : res;
  }, 0);

  return COLOR_MAP[indexOfMin];
}

export function mapToSaturation(hexColor: string) {
  const normalizedComponents = parseHexColor(hexColor, true).map(c => c / 255);
  const diff = Math.max(...normalizedComponents) - Math.min(...normalizedComponents);

  if (diff === 0) {
    return "monochrome";
  } else if (diff < 1 / 4) {
    return "washed";
  } else if (diff < 2 / 4) {
    return "muted";
  }

  return "saturated";
}

export function mapToLightness(hexColor: string) {
  const normalizedComponents = parseHexColor(hexColor, true).map(c => c / 255);
  const sum = normalizedComponents.reduce((acc, val) => acc + val);

  if (sum < 1.2) {
    return "dark";
  } else if (sum < 2) {
    return "medium";
  }

  return "light";
}

function chunk<T>(arr: Array<T>, chunkSize = 1) {
  const result = [];
  let i = 0;

  while (i < arr.length) {
    result.push(arr.slice(i, i + chunkSize));
    i += chunkSize;
  }

  return result;
}