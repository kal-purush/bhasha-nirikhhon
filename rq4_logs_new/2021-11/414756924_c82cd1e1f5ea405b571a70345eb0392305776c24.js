import styled from 'styled-components'

// homepage
export const InfoArticle = styled.article`
width: 90%;
margin: 5vh auto 0vh;
padding: 4rem 0;
gap: 1rem;
display:flex;
justify-content: space-around;
align-items:center;
font-size: 1.2rem.
text-transform: capitalize;
line-height:1;
flex-direction: ${(props) => props.flexItemDirection};
  @media (max-width: 1000px) {
    flex-direction: column-reverse;
    align-items:flex-start;
    padding: 1rem;
  }
}
`

export const ArticleDiv = styled.div`
  display: flex;
  justify-content: center;
  align-items: flex-start;
  text-transform: capitalize;
  flex-direction: column;
  line-height: 1;
`

export const ArticleH2 = styled.h2`
  margin: 1vh 0;
  color: #00005f;
  a:hover {
    cursor: pointer;
  }
`

export const ArticleText = styled.p`
  margin: 3vh auto;
`

export const ArticleLink = styled.a`
  &:hover {
    text-decoration: underline;
    cursor: pointer;
  }
`

export const ArticleImage = styled.img`
  width: 50%;
  @media (max-width: 1000px) {
    width: 60%;
  }
`