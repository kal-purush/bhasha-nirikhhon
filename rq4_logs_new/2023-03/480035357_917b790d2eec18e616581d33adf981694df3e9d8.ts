// TODO: descr
export default function(
  className: string,
  customProperty: { name: string, value?: string | Record<string, unknown> },
  prevClassName?: string,
  prevCustomProperty?: { name: string, value?: string | Record<string, unknown> },
  isLast?: boolean,
): string {
  if (!prevCustomProperty || typeof prevCustomProperty.value !== "object"  || typeof customProperty.value !== "object" ) {
    return '';
  }

  const prevGridClassStrings = `  :root {
    --grid-column-count: var(${prevCustomProperty.name}-column-count);
    --grid-column-gap: var(${prevCustomProperty.name}-column-gap);
    --grid-column-offset: var(${prevCustomProperty.name}-column-offset);
    --grid-max-width: var(${prevCustomProperty.name}-max-width);
  }`

  if (isLast) {
    const gridClassStrings = `  :root {
    --grid-column-count: var(${customProperty.name}-column-count);
    --grid-column-gap: var(${customProperty.name}-column-gap);
    --grid-column-offset: var(${customProperty.name}-column-offset);
    --grid-max-width: var(${customProperty.name}-max-width);
  }`;

    return `\n@media (min-width: ${prevCustomProperty.value.breakpoint}px) and (max-width: ${customProperty.value.breakpoint}px) {
${prevGridClassStrings}
}

@media (min-width: ${customProperty.value.breakpoint}px) {
${gridClassStrings}
}`;
  }

  return `\n@media (min-width: ${prevCustomProperty.value.breakpoint}px) and (max-width: ${customProperty.value.breakpoint}px) {
${prevGridClassStrings}
}`;
}