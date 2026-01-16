import styled from "styled-components";

export const LoaderStyle = styled.div`
    position: fixed;
    top: 0;
    background: #fff;
    z-index: 200;

    width: 100%;
    height: 100%;
    opacity: 0;
    visibility: hidden;
    transition: all .3s ease;

    display: flex;
    justify-content: center;
    align-items: center;

    &.loaderActive{
        opacity: 0.9;
        visibility: visible;       
    }
`