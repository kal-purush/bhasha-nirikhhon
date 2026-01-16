import styled from 'styled-components'

export const Container = styled.div`
   display: flex;
   justify-content: space-evenly;
   align-items: center;
   flex-direction: column;
   width: 100%;
   height: 100%;
`
export const ProjectContainer = styled.div`
   display: flex;
   justify-content: center;
   align-items: center;
   width: 100%;
   gap: 10px;

   a {
      display: flex;
      justify-content: center;
      align-items: center;
      width: 50%;
      svg {
         height: 100%;
      }
   }
   padding: 10px;
`

export const TextContainer = styled.div`
   display: flex;
   justify-content: center;
   align-items: flex-start;
   flex-direction: column;
   width: 50%;
   height: 100%;
   line-height: 15px;
   gap: 10px;
   h2 {
      font-size: 15px;
   }
   p {
      font-size: 12px;
   }
`