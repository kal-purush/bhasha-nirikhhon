import { css } from 'lit';

export default css`
    :host {
        display: flex;
        justify-content: center;
        align-items: center;
        border-radius: 4px;
        padding: 9px 24px;
        cursor: pointer;
        font-family: 'Gotham SSm', sans-serif;
        overflow-wrap: normal;
        font-size: 14px;
        font-weight: 500;
        line-height: 1.2;
        border: 3px solid transparent;
        text-align: center;
        text-transform: inherit;
        backdrop-filter: blur(8px);
        transition: border-color 0.33s ease, background-color 0.33s ease,
            color 0.33s ease, box-shadow 0.25s ease;
    }

    :host([variant='neutral']) {
        background-color: var(--tesla-grey70);
        color: var(--tesla-grey20);
    }

    :host([variant='default']) {
        background-color: var(--tesla-grey75);
        color: var(--tesla-grey20);
    }

    :host([variant='dark']) {
        background-color: var(--tesla-grey15);
        color: var(--tesla-white);
    }

    :host([variant='default']:focus) {
        box-shadow: inset 0 0 0 2px;
        outline: none;
        background-color: var(--tesla-grey65);
    }

    :host([variant='default']:hover) {
        background-color: var(--tesla-grey65);
        backdrop-filter: blur(8px);
    }

    :host([variant='primary']) {
        background-color: var(--tesla-blue30);
        color: var(--tesla-white);
    }

    :host([variant='primary']:hover) {
        background-color: var(--tesla-blue20);
    }

    :host([variant='primary']:focus) {
        box-shadow: inset 0 0 0 2px;
        outline: none;
    }

    :host([variant='danger']) {
        background-color: var(--tesla-red30);
        color: var(--tesla-white);
    }

    :host([variant='warning']) {
        background-color: var(--tesla-yellow);
        color: var(--tesla-white);
    }

    :host([variant='success']) {
        background-color: var(--tesla-green);
        color: var(--tesla-white);
    }

    :host([variant='outline-light']) {
        background-color: transparent;
        color: var(--tesla-white);
        border: 3px solid var(--tesla-white);
    }

    :host([variant='outline-light']:hover) {
        background-color: var(--tesla-white);
        color: var(--tesla-grey10);
    }

    :host([variant='outline-dark']) {
        background-color: transparent;
        color: var(--tesla-grey10);
        border: 3px solid var(--tesla-grey10);
    }

    :host([variant='outline-dark']:hover) {
        background-color: var(--tesla-grey10);
        color: var(--tesla-white);
    }

    :host([rounded]) {
        border-radius: 9999px;
        padding: 0.625rem 1rem;
        font-size: 1rem;
    }

    :host([size='small']) {
        padding: 0 18px;
        height: 32px;
        min-height: 32px;
        font-size: 12px;
        vertical-align: middle;
    }

    :host([disabled]) {
        cursor: not-allowed;
        opacity: 0.5;
    }
`;