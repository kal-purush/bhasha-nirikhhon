import styled from 'styled-components';

export const WrapperSearch = styled.div`
    width: 100%;

    .react-autosuggest__container {
        position: relative;
        width: 100%;
        display: flex;
    }

    .react-autosuggest__input {
        width: 100%;
        padding: 16px 20px;
        font-family: Helvetica, sans-serif;
        font-weight: 300;
        font-size: 16px;
        border: 1px solid inherit;
        border-radius: 4px;
        background: ${props => props.theme.bgColor};
        color: ${props => props.theme.textColor};
    }

    .react-autosuggest__input--focused {
        outline: none;
    }

    .react-autosuggest__input--open {
        border-bottom-left-radius: 0;
        border-bottom-right-radius: 0;
    }

    .react-autosuggest__suggestions-container {
        display: none;
    }

    .react-autosuggest__suggestions-container--open {
        display: block;
        position: absolute;
        top: 51px;
        width: 100%;
        border: 1px solid #aaa;
        background-color: inherit;
        font-family: Helvetica, sans-serif;
        font-weight: 300;
        font-size: 16px;
        border-bottom-left-radius: 4px;
        border-bottom-right-radius: 4px;
        z-index: 2;
    }

    .react-autosuggest__suggestions-list {
        margin: 0;
        padding: 0;
        list-style-type: none;
        background: white;
    }

    .react-autosuggest__suggestion {
        cursor: pointer;
        padding: 10px 20px;
    }

    .react-autosuggest__suggestion--highlighted {
        background-color: #ddd;
    }
`;