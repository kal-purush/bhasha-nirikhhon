import styled from "styled-components";

export const PostConteiner = styled.div `
display: flex;
justify-content: center;
flex-direction: row;
font-family: sans-serif;
background-color:#F0F0F0;

`
export const LogoConteiner = styled.div `
display: flex;
justify-content: center;
flex-direction: column;
border: 1px gray solid;
height:30%;
width:20%;
padding:5%;
margin:10%;
margin-right:5%;
background-color:#FCFCFC;
box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
`
export const CommentConteiner = styled.div `
display: flex;
justify-content: center;
flex-direction: column;

height:30%;
width:30%;
text-align: center;

margin:10%;
margin-left:5%;
`
export const ImgBox = styled.div`
  display:flex;
  justify-content: center;
  align-content:center;
  
`;

export const ButtonFeed = styled.button`
  outline: none;
  width: 10vw;
  height: 5vh;
  background-color: #FF5700;
  border-radius: 5vw;
  border: none;
  padding: 5px;
  cursor: pointer;
  font-size: 16px;
  color:white;

  
  top: 120%;
  right: 0;
  left: 0;
  margin-right: auto;
  margin-left: auto;

  :hover {
    background-color: #55ACEF;
    transform: translateY(-3px);
    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.2);
  }

  :active {
    transform: translateY(-1px);
    box-shadow: 0 5px 10px rgba(0, 0, 0, 0.2);
  }
`;

export const ButtonSignOut = styled.button`
  border: none;
  background-color: transparent;
  text-decoration: underline;
  cursor: pointer;
  font-size: 14px;
  
  
  top: 140%;
  right: 0;
  left: 0;
  margin-right: auto;
  margin-left: auto;
`;

export const ShareLogos = styled.div`
  display:flex;
  justify-content: center;
  align-content:center;
  
`;

export const CommentPost = styled.div `
display: flex;
justify-content: center;
flex-direction: column;
border: 1px gray solid;
height:20%;
width:100%;
text-align: center;

background-color:#FCFCFC;
box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);


`
export const CommentWrite = styled.div `
display: flex;
justify-content: center;
flex-direction: column;
border: 1px gray solid;
height:20%;
width:100%;
text-align: center;
margin-top:10px;
padding-top:10px;
padding-bottom:10px;

background-color:#FCFCFC;
box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
`
export const ButtonPost = styled.button`
  outline: none;
  width: 10vw;
  height: 5vh;
  background-color: #FF5700;
  border-radius: 5vw;
  border: none;
  padding: 5px;
  cursor: pointer;
  font-size: 16px;
  color:white;

  
  top: 120%;
  right: 0;
  left: 0;
  margin-right: auto;
  margin-left: auto;
`
export const CommentsList = styled.div `
display: flex;
justify-content: center;
flex-direction: column;
border: 1px gray solid;
height:10%;
width:100%;
text-align: center;
margin-bottom:10px;
list-style-type: none;
background-color:#FCFCFC;
box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);


`