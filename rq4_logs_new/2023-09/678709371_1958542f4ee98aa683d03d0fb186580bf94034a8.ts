export const pluralizeWithUppercase = (
  name = "",
  capitalizeFirstLetter = true
): string => {
  const normalized = name.trim();

  if (capitalizeFirstLetter) {
    const firstLetterUppercase =
      normalized.charAt(0).toUpperCase() + normalized.slice(1);

    if (normalized.toLowerCase().endsWith("s")) {
      return `${firstLetterUppercase}'`;
    } else {
      return `${firstLetterUppercase}s`;
    }
  } else {
    if (normalized.toLowerCase().endsWith("s")) {
      return `${normalized}'`;
    } else {
      return `${normalized}s`;
    }
  }
};