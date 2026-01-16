import styled from "styled-components";
import { appearFromRight } from "../../styles/animations";

export const StyledRedefinePassword = styled.div`
    height: fit-content;
    min-height: 100vh;
    width: 100%;
    display: flex;
    flex-flow: column nowrap;
    justify-content: space-between;
    background-color: var(--grey-8);

    form {
        align-self: center;
        width: 91.5%;
        max-width: 412px;
        padding: 44px 48px;
        background-color: var(--grey-10);
        border-radius: 4px;
        margin: 122px 0;

        display: flex;
        flex-flow: column nowrap;
        align-items: center;
        gap: 24px;

        animation: ${appearFromRight} 1s;
    }

    h2 {
        color: var(--black);
        font-family: "Lexend", sans-serif;
        font-size: 24px;
        margin-bottom: 8px;
        align-self: flex-start;
    }
`;

export const StyledSuccessModal = styled.div`
    margin-top: 22px;
    display: flex;
    flex-flow: column nowrap;
    gap: 20px;
    max-width: 472px;
    width: 83.73vw;

    .modalSuccess__h3--subtitle {
        color: var(--grey-1);
        font-size: 16px;
        font-weight: 500;
        font-family: "Lexend", sans-serif;
    }
    .modalSuccess__p--description {
        color: var(--grey-2);
        font-size: 16px;
        font-weight: 400;
        font-family: "Inter", sans-serif;
        margin-bottom: 13px;
        line-height: 28px;
    }
`;