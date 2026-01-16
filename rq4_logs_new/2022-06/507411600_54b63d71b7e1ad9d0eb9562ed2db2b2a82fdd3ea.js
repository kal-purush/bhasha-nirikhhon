import styled from "styled-components";
import { keyframes } from "styled-components";

// animations 
const size = keyframes`
 50% { font-size: 60px }
 100% { font-size: 30px }
 
`
const rotation = keyframes`
 50%{transform: rotate(180deg);}
 100%{transform:rotate(360deg)}
`

export const Navbar = styled.nav`
    position: fixed;
    top: 0 ; 
    left: 0 ;
    background-color: #000; 
    width: 250px;
    height: 100vh;

`
export const NavbarHead = styled.div`
    width: 100%;
    margin-top: 20px;
    display: flex;
    justify-content: space-around;
    align-items: center;
    
`
export const ImageLogo = styled.img`
    width : 50px ; 
    height: 50px ;
    border-radius: 50%;
    
    `

export const ButtonToggle = styled.button`
    width: 50px;
    height: 50px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: transparent;
    color:white ; 
    font-size: 30px;
    animation-name: ${size};
    animation-duration: 1.5s;
    animation-iteration-count: infinite;
`

export const NavbarBody  = styled.ul`
    list-style: none;
    color : white ; 
    margin: 20px ;  
    font-size: 23px;
    
`

export const NavbarLinks = styled.li`
    margin-top: 10px;
    text-transform: capitalize;
    cursor: pointer;
    padding:20px ; 
`

export const Container = styled.div`
    position: absolute;
    top:30% ; 
    left:45% ; 
    width :500px ;
    text-align: center;

`

export const ReactImage = styled.img `
    width: 300px;
    height: 300px;
    animation-name: ${rotation};
    animation-duration: 5s;
    animation-iteration-count: infinite;
`

export const Header = styled.h2`
    font-size: 64px;
    color:black  ; 
`
export const ChangeButton = styled.button`
    font-size: 30px;
    background-color: black;
    color:white; 
    position: absolute;
    top: 10px ; 
    right: 10px ; 
`