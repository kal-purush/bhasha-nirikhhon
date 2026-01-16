import styled from "styled-components"

export const HeaderStyled = styled.header `
background-color: #000000;
color: #FFFAFA;
display: flex;
width: 100%;
height: 60px;
padding-left: 200px;
position: fixed;


h1 {
    margin-top: 10px;
}

ul {
    list-style: none;
    display: flex;
    font-family: Inter;
    gap: 50px;
    margin-left: 40px;
    font-size: 20px;
}

li {
    margin-top: 19px;
}

input {
    margin-left: 10px;
    padding-left: 10px;
    margin-top: 10px;
    height: 40px;
    font-size: large;
    color: white;
    background-color: #000000;
    border: none;
    width: 500px;
    outline: #000000;
}

//classNames de "Links"
a.menu {
    color: #FFFAFA;
    text-decoration: none;
    position: relative;

    &::after {
      content: '';
      width: 0%;
      height: 2px;
      background-color: #FFA500;
      position: absolute;
      bottom: 0;
      left: 0;
      transition: 0.6s ease-in-out;
   }

   &:hover::after {
      width: 100%;
   }
}

a.carinho {
    color: #FFFAFA;
    text-decoration: none;
    margin-left: 260px;
}

`