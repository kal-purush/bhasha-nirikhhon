const to_number_of_seconds = (timeString: string) => {
  const s = 1,
    m = 60 * s,
    h = 60 * m,
    d = 24 * h;
  const timeElementsJson = { s, m, h, d };
  const TimeElements = timeString.split(":");
  return TimeElements.map((timeElement) => {
    const comp = timeElement.slice(-1) as "s" | "m" | "h" | "d";
    return timeElementsJson[comp] * +timeElement.slice(0, -1);
  }).reduce((a, b) => a + b, 0);
};

export {to_number_of_seconds};