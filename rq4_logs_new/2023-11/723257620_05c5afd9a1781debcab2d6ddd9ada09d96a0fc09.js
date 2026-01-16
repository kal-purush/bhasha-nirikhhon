import styled from 'styled-components';

export const Dropdown = styled.select`
    padding: 10px;
    margin: 0 10px;
    border: 2px solid #ccc;
    border-radius: 5px;
    font-size: 16px;
    outline: none;
    cursor: pointer;
    transition: border-color 0.3s ease;

    &:hover {
        border-color: #888;
    }

    &:focus {
        border-color: #555;
    }
`;