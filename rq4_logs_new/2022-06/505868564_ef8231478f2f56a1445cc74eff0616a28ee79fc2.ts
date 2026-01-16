import styled from "styled-components"


interface Props {
  img:string
}

const Container = styled.section`
  display: grid;
  grid-template-columns: repeat(2,1fr);
  width: 100%;
  height: 70%;
  opacity: 0;
  transform: translateX(-40px);
  animation: .5s ToRight forwards;

  @keyframes ToRight {
    to {
      transform: initial;
      opacity: initial;
    }
  }

`
const InfoPerson = styled.div`
  display: flex;
  flex-direction: column;
  margin: 5rem 0;
`
const Title = styled.div`
  display: flex;
  flex-direction: column;
`
const Role = styled.span`
  font : var(--font-default2);
  text-transform: uppercase;
  letter-spacing: 1.5px;
  font-weight: bold;
  color: var(--light);
  `
const Name = styled.span`
  font : var(--font-default0);
  text-transform: uppercase;
  letter-spacing: 1.5px;
  margin-bottom: 2rem;
`

const Bio = styled.p`
  max-width: 45ch;
  color: var(--light2);
  line-height: 1.8;
  letter-spacing: .5px;
`

const ImgCrew = styled.div<Props>`
  height: 137.6%;
  width: 100%;
  background: transparent url(${props => props.img}) bottom no-repeat;
  background-size: contain;
  opacity: 0;
  transform: translateX(-50px);
  animation: .8s ToRight forwards;

  @keyframes ToRight {
    to {
      transform: initial;
      opacity: initial;
    }
  }

`
export {
  Container,
  InfoPerson,
  Title,
  Role,
  Name,
  Bio,
  ImgCrew,


}