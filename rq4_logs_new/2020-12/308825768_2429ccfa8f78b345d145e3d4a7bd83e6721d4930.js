import styled from 'styled-components';
import { Button } from './variables';

const ConfirmWindow = styled.div`
    position: absolute;

    width:100%;
    height:100%;

    display:flex;
    flex-direction:column;
    justify-content: center;
    align-items:center;

    background-color:rgba(255,255,255,0.9);
    transform: translateY(2rem);
    transition: all ease .5s;
    opacity: 0;
    visibility: hidden;
    .txt{
        font-size: 1rem;
        color: black;
        padding:1rem 2rem;
    }
    button {
        cursor:pointer;
        outline: none;
        background: #fff;
        color: black;
        border:2px solid black;
        padding:0.3rem 0.8rem;
        margin:0 0.1rem;
        border-radius:${Button.radius};
        &:hover {
            outline: none;
            background: tomato;
            color: #fff;
            border:2px solid tomato;
        }
    }
    &.visible {
        opacity: 1;
        visibility: visible;
        transform: translateY(0rem);
    }
`

export { ConfirmWindow };