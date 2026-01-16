import styled from 'styled-components'


export const DownloadContainer = styled.div`
  height: 600px;
  display: grid;
justify-content: center;
margin-right: auto;
margin-left: auto;

background: #fff;
padding: 100px 0 0 100px;

@media screen and (max-width:830px){
    padding-top: 50px;
}
`
export const DownloadWrapper = styled.div`
margin-bottom: 80px;
background-color: #fff;
 display: grid;
  grid-auto-columns: minmax(auto, 1fr);
  align-items: center;
  grid-template-areas: "col2 col1";

  @media screen and (max-width: 830px) {
    display: block;
    margin-top: 100px;

  }

`
export const Form = styled.form`
`
export const DownloadInfo = styled.input`
padding: 10px 150px 10px 0;

@media screen and (max-width: 470px ){
    padding: 10px 80px 10px 0;

}
`
export const NotifyButton = styled.button`
border:none;
border-radius: 2px;
  position: relative;
  top: 40px;
  font-size: 20px;
  text-decoration: none;
  font-weight: 500;
  color: #000;
  background-color: #fca311;
  padding: 10px;
  margin: 10px -10px -50px 1px; 

  &:hover {
    transition: 0.1s ease-in-out;
    opacity: 80%;
  }

`
export const Image = styled.img`
width: 100%;
@media screen and (max-width:830px){
    display: flex;
    justify-content: center;
    position: relative;
    bottom:40px;
    right: 50px;
    width: 80%;
    padding-bottom: 50px;
    
}
@media screen and (max-width:470px){
    width: 250px;
    padding-bottom: 50px;
}
`

export const Label = styled.label`
    color: #000;
    font-size: 20px;
    @media screen and (max-width: 470px ){
        font-size: 15px;

}
`
export const ContentH1 = styled.h1`
color: #5C667B;
@media screen and (max-width: 830px ){
        font-size: 15px;

}
`;