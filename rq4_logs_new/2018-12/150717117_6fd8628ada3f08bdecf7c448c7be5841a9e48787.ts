import styled, { css } from './style-theme'

export const truncate = (width?: number) => {
  return `
    width: ${width};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  `
}

const sizes = {
  desktop: 992,
  tablet: 768,
  mobile: 576
}

export const media = Object.keys(sizes).reduce((acc, key) => {
  acc[key] = (args: any) => css`
    @media (max-width: ${sizes[key] / 16}em) {
      ${css(args)};
    }
  `
  return acc
}, {} as Record<keyof typeof sizes, (args: any) => string>)

/*
  1. Layout Properties (position, float, clear, display)
  2. Box Model Properties (width, height, margin, padding)
  3. Visual Properties (color, background, border, box-shadow)
  4. Typography Properties (font-size, font-family, text-align, text-transform)
  5. Misc Properties (cursor, overflow, z-index)
*/

const _example = styled.div`
  .example {
    /* layout */
    display: inline-block;

    /* box-model */
    margin: 1em 0;
    padding: 1em 4em;

    /* visual */
    color: #fff;
    background: #196e76;
    border: 0.25em solid #196e76;
    box-shadow: inset 0.25em 0.25em 0.5em rgba(0, 0, 0, 0.3), 0.5em 0.5em 0 #444;

    /* typography */
    font-size: 3em;
    font-family: Avenir, Helvetica, Arial, sans-serif;
    text-align: center;
    text-transform: uppercase;
    text-decoration: none;

    /* miscellaneous */
    z-index: 8;
  }
`

_example