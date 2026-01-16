import styled from "styled-components"

export const BodyStyled = styled.body `
display: flex;
flex-direction: column;
min-height: 100vh;
background-color: #EFF2F6;
`

export const MainStyled = styled.header `
margin-top: 60px;
flex: 1 1 0;
display: flex;
justify-content: center;
height: 100vh;
`

export const FormStyled = styled.form `
background-color: #FFFF;
display: flex;
align-items: center;
flex-direction: column;
padding-top: 100px;




h1{
align-items: center;
display: flex;
font-size: 50px;
font-weight: 600px;
size:56px;  
padding-bottom: 30px;
}

p{
text-align: center;
font-weight: 400;
font-size:20px;
width: 610px;
height: 80px; 

}

img{
padding: 30px 30px;
padding-bottom: 60px;
}

button{
border-color: #000000;
background-color: #FFFFFF;
color: #000000;
font-weight: bolder;
font-size: 18px;
padding: 10px 10px;
width: 150px;

cursor: pointer;

&:hover {
    color: #FFA500;
    transition: 0.9s;
}
}
`

export const CompaniesStyled = styled.form `
display: inline-flex;
flex-direction: row;
align-items:center;
padding-bottom: 100px;
padding-top: 650px;
margin-left: -830px;


img{
align-content:space-evenly;
max-width: 400px;
height: 60px;
}
`