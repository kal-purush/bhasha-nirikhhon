import { styled } from "styled-components"


export const Container = styled.section`
    width: 100%;
    height: 95vh;
    color: black;
    display: flex;
    justify-content: center;
    align-items: center;
`

export const Header = styled.header`
    width: 100%;
    height: 5vh;
    color: black;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 20px;
    font-size: 15px;
`

export const Contents = styled.div`
    width: 90%;
    height: 80%;
    display: flex;
    flex-direction: column;
`

export const Title = styled.h2`
    font-size: 30px;
    text-align: center;
    margin-bottom: 30px;
`


export const EmailInput = styled.input`
    background-color: gainsboro;
    border: none;
    border-radius: 10px;
    height: 45px;
    color: black;
    font-size: 20px;
`

export const EmailList = styled.div`
    height: 45px;
    width: 100%;
    display: flex;
    justify-content: flex-start;
    align-items: center;
    overflow-x: scroll;
    font-size: 20px;
    margin-bottom: 30px;
`

export const EmailShortcut = styled.span`
    background-color: gainsboro;
    border-radius: 10px;
    padding: 5px;
    display: flex;
`


export const SubmitButton = styled.button`
    height: 45px;
    background-color: blue;
    border: none;
    border-radius: 10px;
    font-size: 20px;
`