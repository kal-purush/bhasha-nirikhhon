import styled from "styled-components";

export const StyledModalEditUser = styled.div`
    border-radius: 8px;
    display: flex;
    flex-flow: column nowrap;
    gap: 18px;
    background-color: var(--white-fixed);
    font-family: "Inter";
    font-style: normal;
    max-width: 472px;
    width: 83.73vw;

    .containerButtonsFinal {
        gap: 10px;
        display: flex;
        justify-content: flex-end;
        margin-top: 5px;
    }

    label {
        margin-top: 24px;
    }

    .disableButton {
        font-family: "Inter";
        font-style: normal;
        max-height: 48px;
        height: 48px;
        box-sizing: border-box;
        display: flex;
        flex-direction: row;
        justify-content: center;
        align-items: center;
        font-weight: 600;
        gap: 10px;
        background-color: var(--brand-3);
        border: none;
        color: var(--brand-4);
        width: 193px;
        border-radius: 4px;
        font-size: 14px;
        cursor: not-allowed;
    }
`;