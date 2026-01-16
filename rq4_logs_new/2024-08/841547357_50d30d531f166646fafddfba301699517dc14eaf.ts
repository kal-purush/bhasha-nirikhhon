import styled from "styled-components";


export const ModalOverlay = styled.div<{ $isOpen: boolean }>`
    display: ${({ $isOpen }) => ($isOpen ? "flex" : "none")};
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.75);
    justify-content: center;
    align-items: center;
    z-index: 1000;
    padding:1rem;

`;

export const ModalContainer = styled.div`
    width: 100%;
    max-width: 728px;
    height:100%;
    max-height:377px;
    background: white;
    padding: 20px;
    border-radius: 24px;
    text-align: center;
    gap:50px;

    display:flex;
    justify-content:center;
    align-items:center;
    flex-direction:column;

    @media(max-width:470px){
        padding:1rem;
    }
`;

export const ModalDescription = styled.p`
    font-size: 2.5rem;
    font-weight:300;
    line-height:46.88px;
    text-align:center;
    margin-bottom: 20px;
    color:${({ theme }) => theme.colors.textColorModal};
    @media(max-width:470px){
        padding:1rem;
    }
`;

export const ButtonGroup = styled.div`
    display: flex;
    justify-content: center;
    gap: 70px;
    width:100%;

    @media(max-width:470px){
        gap:20px;
    }
`;

export const Button = styled.button`
    padding: 10px 20px;
    border: none;
    border-radius: 10px;
    cursor: pointer;
    font-size: 2.62rem;
    font-weight:500;
    line-height:49.22px;
    width:100%;
    max-width:248px;
    height:100%;
    max-height:73px;


    &:first-child {
        background: ${({ theme }) => theme.colors.greenColor};
        color: ${({ theme }) => theme.colors.whiteColor};
        width:100%;
        max-width:248px;
    }

    &:last-child {
        background: ${({ theme }) => theme.colors.redColor};
        color: ${({ theme }) => theme.colors.whiteColor};;
    }
`;