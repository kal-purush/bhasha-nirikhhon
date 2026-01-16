import styled, { css } from 'styled-components'
import media from 'styled-media-query'

export const Wrapper = styled.li`
  ${({ theme }) => css`
    cursor: pointer;
    list-style: none;
    background: ${theme.colors.black};
    transition: all 0.5s ease-in-out;
    color: ${theme.colors.white};
    font-weight: ${theme.font.bold};
    border-radius: 4px;

    padding: ${theme.spacings.xsmall} ${theme.spacings.xsmall};

    &:hover {
      color: ${theme.colors.secondary};
    }

    font-size: ${theme.font.sizes.large};

    ${media.greaterThan('medium')`
      font-size: ${theme.font.sizes.xlarge};
    `}
  `}
`