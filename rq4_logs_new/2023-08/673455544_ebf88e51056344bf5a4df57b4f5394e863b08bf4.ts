import { styled } from "styled-components";

export const DeleteModalWrapper = styled.div`
    position: fixed;
    top: 0px;
    left: 0px;
    height: 100vh;
    width: 100vw;
    background-color: rgba(0,0,0,0.5);
    z-index: 20;
`
export const DeleteModalContainer = styled.div`
    width: 90%;
    max-width: 500px;
    background-color: var(--white);
    border-radius: 8px;
    margin: 5rem auto;
    padding: 1rem;
    display: flex;
    flex-direction: column;

    > h4 {
        color: var(--black);
        font-weight: 700;
        font-size: .9rem;
        margin-bottom: 1.5rem;
    }

    > p {
        color: var(--gray);
        font-size: .9rem;
        margin-bottom: 2rem;
    }
`

export const DeleteModalHeader = styled.div`
    position: relative;
    /* display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: ; */
    margin-bottom: 2rem;

    > h3 {
        color: var(--black);
        font-size: .9rem;
    }

    > span {
        position: absolute;
        top: 0px;
        right: 10px;
        color: var(--light-gray);
        cursor: pointer;
        transition: .2s;

        &:hover {
            color: var(--gray);
        }
    }
`
export const DeleteModalButtons = styled.div`
    width: 75%;
    align-self: flex-end;
    justify-content: space-between;
    display: flex;

    > button {
        font-weight: 700;
        border-radius: 8px;
        cursor: pointer;
        transition: .5s;
        height: 2.5rem;
    }

    > .cancel {
        background-color: #DEE2E6;
        color: #495057;
        width: 39%;

        &:hover {
            background-color: #495057;
            color: #DEE2E6;
        }
    }

    > .delete {
        background-color: #FDD8D8;
        color: #495057;
        width: 59%;

        &:hover {
            background-color: #495057;
            color: #FDD8D8;
        }
    }
`