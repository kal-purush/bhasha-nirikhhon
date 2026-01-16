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
    font-size: 20px;
    text-align: center;
    margin-bottom: 30px;
`

export const Descriptions = styled.p`
    text-align: center;
    font-size: 15px;
    margin-bottom: 10px;
`


export const AuthInput = styled.input`
    background-color: gainsboro;
    border: none;
    border-radius: 10px;
    height: 45px;
    color: black;
    font-size: 20px;
    margin-bottom: 10px;
`

export const SubmitButton = styled.button`
    height: 45px;
    background-color: blue;
    border: none;
    border-radius: 10px;
    font-size: 20px;
`