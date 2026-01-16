import { css } from 'lit';

export default css`
    :host {
        line-height: 20px;
    }

    a {
        -webkit-backdrop-filter: unset;
        background-color: transparent;
        white-space: nowrap;
        display: inline-flex;
        align-items: center;
        border-radius: 4px;
        color: #171a20;
        font-weight: 500;
        inline-size: auto;
        padding: 4px 8px;
        transition: background-color 0.5s ease;
        text-decoration: none;
        font-family: 'Gotham SSm', sans-serif;
        font-size: 14px;
    }

    a:hover {
        background-color: var(--tesla-grey30);
    }

    span {
        margin: 0 8px;
    }
`;