import { css } from '@emotion/react';

export const logo = css`
    display: flex;
    align-items: center;
    padding: 10px;
`;

export const logoImg = css`
    height: 75px;
    margin-right: 20px;
    pointer-events: none;
`;

export const header = css`
    background-color: #282c34;
    color: white;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 20px;
`;

export const content = css`
    padding: 20px;
`;

export const button = css`
    background-color: #282c34;
    color: white;
    padding: 20px;
    border: none;
    border-radius: 5px;
    font-size: 16px;
    font-weight: 600;
    outline: none;
    cursor: pointer;
    &:hover {
        background-color: white;
        color: #282c34;
    }
`;

export const title = css`
    font-weight: 700;
    font-size: 24px;
`;