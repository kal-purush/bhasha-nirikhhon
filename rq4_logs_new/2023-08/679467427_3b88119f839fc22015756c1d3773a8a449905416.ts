import styled from "styled-components"

export const FooterStyled = styled.footer `
background-color: #FFFFFF;
display: flex;
gap: 13vh;
justify-content: center;
width: 100%;
padding-bottom: 10px;
padding-top: 20px;

div{
    gap: 10px;
}

h1 {
    margin-bottom: 15px;
    font-size: 40px;
}

p {
   max-width: 420px;
   margin-bottom: 20px;
   font-weight: bolder;
}

input {
    padding: 10px;
    width: 440px;
    outline: #000000;
}

button {
    background-color: #FFFFFF;
    border: none;
    padding: 11px;
    color: #000000;
    position: relative;
    right: 80px;
    font-weight: bolder;
    cursor: pointer;
    font-size: 14px;

    &:hover {
        color: #FFA500;
        transition: 0.4s;
    }
}

h2 {
    margin-bottom: 20px;
}

li {
    list-style: none;
    opacity: 70%;
    margin-bottom: 7px;
}

`