import styled from 'styled-components';

export const ButtonStyled = styled.button`
    padding: 0.8em 1.7em;
    background-color: transparent;
    border-radius: 0.3em;
    position: relative;
    overflow: hidden;
    cursor: pointer;
    transition: 0.5s;
    font-weight: 400;
    font-size: 17px;
    border: 1px solid #61dafb;
    text-transform: uppercase;
    color: #61dafb;
    z-index: 1;
    margin-top: 25px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 5px;

    &::before,
    &::after {
        content: '';
        display: block;
        width: 50px;
        height: 50px;
        transform: translate(-50%, -50%);
        position: absolute;
        border-radius: 50%;
        z-index: -1;
        background-color: #61dafb;
        transition: 1s ease;
    }

    &::before {
        top: -1em;
        left: -1em;
    }

    &::after {
        left: calc(100% + 1em);
        top: calc(100% + 1em);
    }

    &:hover::before,
    &:hover::after {
        height: 410px;
        width: 410px;
    }

    &:hover {
        color: #333;
    }

    &:active {
        filter: brightness(0.8);
    }
`;