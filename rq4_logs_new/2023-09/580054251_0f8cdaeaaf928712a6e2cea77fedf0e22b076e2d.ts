import styled from "@emotion/styled";

export const AuthorS = styled.div(({theme}) => `
  padding: 40px;
  background-color: rgba(255, 255, 255, 0.1);
  > .author-top{
    display: flex;
    margin-bottom: 25px;
  }
  border-radius: ${theme.borderRadius['base']};
  ${theme.breakpoints.down('xxl')} {
    border-radius: ${theme.borderRadius['xxl']};
  }
  ${theme.breakpoints.down('xl')} {
    border-radius: ${theme.borderRadius['xl']};
  }
  ${theme.breakpoints.down('lg')} {
    border-radius: ${theme.borderRadius['lg']};
  }
  ${theme.breakpoints.down('sm')} {
    border-radius: ${theme.borderRadius['sm']};
  }
`)

export const Name = styled.div(({theme}) => `
  display: flex;
  align-items: center;
  span{
    margin-left: 20px;
    font-size: 20px;
    font-weight: bold;
  }
`)