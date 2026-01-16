import React, { useEffect, useRef } from "react"
import styled from "styled-components"
import { useObserver } from "../../utils/Oberservers"
import "./TransactionCard.scss"

const Card = styled.div`
    background: #333333;
    box-shadow: 0px 10px 10px rgba(0, 0, 0, 0.25);
    border-radius: 40px;
    padding-left: 1rem;
    padding-right: 1rem;
    height: 10vh;
    z-index:1040;
    transform: translateY(-60%);
    display: grid;
    grid-template-columns: repeat(11, 1fr);
    grid-template-rows: repeat(3, 1fr);
    grid-template-areas: 
    ". a a a a . c c c c ."
    ". d d d d . t t t t ."
    ". . . . . b . . . . .";
    align-items: start;
    justify-items: center;
    margin-bottom: 1vh;
    opacity: 1;
`
export default function TransactionCard () {
    const DOMcard = useRef(null)
    const observer = useObserver(DOMcard, 1, ["transaction-card__hidden","nonAnimated"])
    useEffect( () =>{
        observer.observe(DOMcard.current)
    })

    return (
        <Card ref={DOMcard}></Card>
    )
}