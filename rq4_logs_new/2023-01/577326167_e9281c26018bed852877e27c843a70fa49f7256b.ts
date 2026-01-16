import styled from "styled-components";

export const StyledModalUpdate = styled.div`
    border-radius: 8px;
    display: flex;
    flex-flow: column nowrap;
    gap: 18px;
    background-color: var(--white-fixed);
    font-family: "Inter";
    font-style: normal;
    max-width: 472px;
    width: 83.73vw;

    label {
        margin-top: 24px;
    }

    .containerButtons {
        gap: 10px;
        display: flex;
        width: 100%;
    }
    .containerButtonsFinal {
        gap: 10px;
        display: flex;
        justify-content: space-between;
        margin-top: 55px;
    }

    .updateProductModal__div--numbersContainer {
        width: 100%;
        max-width: 472px;
        flex-flow: row wrap;
        display: flex;
        gap: 11px;
    }
    .updateProductModal__div--numbersContainer div:nth-child(3) {
        label {
            margin-top: 13px;
        }
    }

    @media (min-width: 425px) {
        .updateProductModal__div--numbersContainer {
            flex-flow: row nowrap;
        }
        .updateProductModal__div--numbersContainer div:nth-child(3) {
            label {
                margin-top: 24px;
            }
        }
    }

    #veichleTypeId {
        margin-bottom: 18px;
    }
    .published {
        margin-top: 24px;
        margin-bottom: 18px;
    }

    .disableButtonImage {
        margin-top: 24px;
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
        background-color: var(--brand-4);
        border: none;
        color: var(--brand-1);
        width: 100%;
        border-radius: 4px;
        font-size: 14px;
        cursor: not-allowed;
    }

    @media (max-width: 425px) {
        #deleteAd {
            font-size: 15px;
        }
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
        width: 50%;
        border-radius: 4px;
        font-size: 14px;
        cursor: not-allowed;
    }
    @media (min-width: 425px) {
        .disableButton {
            width: 41%;
        }
    }

    #imagesSpan {
        margin-bottom: 24px;
    }

    #AddFieldImg {
        margin-top: 24px;
    }

    .buttonDeleteInput {
        display: flex;
        padding: 0;
        margin: 0;
        margin-top: 25px;
        width: 85%;
        justify-content: flex-end;
        position: absolute;
        border: 0;
        background-color: transparent;
        line-height: 0;
        margin-right: 6px;
    }

    @media (min-width: 668px) {
        .disableButtonImage {
            width: 318px;
        }
    }
`;

export const StyledConfirmDeleteModal = styled.div`
    margin-top: 7px;
    display: flex;
    flex-flow: column nowrap;
    gap: 25px;
    max-width: 472px;
    width: 83.73vw;

    .modalConfirmDelete__h3--subtitle {
        color: var(--black);
        font-size: 16px;
        font-weight: 500;
        font-family: "Lexend", sans-serif;
    }
    .modalConfirmDelete__p--description {
        color: var(--grey-2);
        font-size: 16px;
        font-weight: 400;
        font-family: "Inter", sans-serif;
        margin-bottom: 18px;
        line-height: 28px;
    }
    .modalConfirmDelete__div--container {
        width: 100%;
        display: flex;
        gap: 10px;
        justify-content: flex-end;
    }
    @media (max-width: 560px) {
        .modalConfirmDelete__div--container {
            button:nth-child(1) {
                width: 36%;
            }
            button:nth-child(2) {
                width: 60.8%;
            }
        }
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