import Styled, { css } from 'styled-components';
export const Container = Styled.div``;

export const PostCardCover = Styled.div`
margin-bottom: ${({ theme }) => theme.spacings.small};
transition: opacity 300ms ease-in-out;
&:hover {
  opacity: 0.5;
}
img{
  max-width: 100%;
  display: block;
  object-fit: cover;
  height: 150px;
  margin: 0 auto;
}
`;
export const PostCardHeading = Styled.h2`

${({ theme }) => css`
  font-size: ${theme.font.sizes.medium};
  a {
    color: ${theme.colors.darkGray};
  }
`}
:first
`;

export const Published = Styled.div`
 ${({ theme }) => css`
   small {
     color: ${theme.colors.darkGray};
   }
   small:first-child {
     padding-right: ${theme.spacings.small};
   }
 `};
`;