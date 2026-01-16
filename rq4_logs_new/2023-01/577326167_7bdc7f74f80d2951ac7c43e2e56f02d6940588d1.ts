import styled from "styled-components";

export const StyledModalCreate = styled.div`
    padding: 0 16px 25px 16px;
    border-radius: 8px;
    display: flex;
    flex-flow: column nowrap;
    gap: 18px;
    background-color: var(--white-fixed);
    font-family: "Inter";
    font-style: normal;
    max-width: 520px;
    width: 100vw;

    label {
        margin-top: 24px;
    }

    .modal__header {
        max-width: 487px;
        width: 100%;
        height: 56px;

        display: flex;
        justify-content: space-between;
        align-items: center;

        h3 {
            color: #212529;
            font-weight: 500;
            font-size: 16px;
            font-family: "Lexend", sans-serif;
        }
        button {
            padding: 0;
            margin: 0;
            border: 0;
            background-color: transparent;
            line-height: 0;
            margin-right: 6px;

            img {
                width: 12px;
                height: 12px;
            }
        }
    }
    .containerButtons {
        gap: 10px;
        display: flex;
        width: 100%;
    }
    .containerButtonsFinal {
        gap: 10px;
        display: flex;
        justify-content: flex-end;
        margin-top: 40px;
    }

    #veichleTypeId {
        margin-bottom: 18px;
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

    #imagesSpan {
        margin-bottom: 24px;
    }

    #AddFieldImg {
        margin-top: 24px;
    }
`;