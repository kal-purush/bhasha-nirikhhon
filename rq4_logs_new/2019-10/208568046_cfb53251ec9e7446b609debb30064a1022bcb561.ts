import { Link } from "gatsby"
import styled from '@emotion/styled'
import theme from 'styles/theme'

export const Wrapper = styled.header`
  position: absolute;
  left: 0;
  right: 0;
  height: 6rem;
  z-index: 1;
`

export const Inner = styled.div`
  max-width: 1200px;
  margin: 0 auto;
  display: flex;
  align-items: center;
  height: 100%;
  justify-content: space-between;
`

export const Logo = styled(Link)`
  font-weight: bold;
  text-decoration: none;
  color: ${theme.color.textDark};
`

export const Nav = styled.nav`
  ul {
    list-style: none;
    padding: 0;

    li {
      margin-left: 3rem;
      display: inline-block;

      a {
        text-decoration: none;
        color: ${theme.color.textDark};

        &:hover {
          color: ${theme.color.link};
        }
      }
    }
  }
`