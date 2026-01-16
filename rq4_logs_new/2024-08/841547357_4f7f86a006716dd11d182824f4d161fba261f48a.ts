import styled from 'styled-components';

export const Container = styled.div`
    position: relative;
    width: 100%;
`;

export const ToggleDiv = styled.div<{ $hasError?: boolean }>`
    display: flex;
    align-items: center;
    border: 1px solid ${({ $hasError, theme }) => ($hasError ? theme.colors.redColor : theme.colors.colorInputBorder)};
    border-radius: 8px;
    background-color: ${({ theme }) => theme.colors.whiteColor};
    padding: 10px;
    cursor: pointer;
    user-select: none;
    justify-content:space-between;
    color:${({ theme }) => theme.colors.darkGray};

    &:focus-within {
        border-color: ${({ $hasError, theme }) => ($hasError ? theme.colors.redColor : theme.colors.sucessInput)};
    }

    div{
        display:flex;
        gap:18px
    }
`;

export const CheckboxContainer = styled.div`
    position: absolute;
    top: 100%;
    left: 0;
    width: 100%;
    border: 1px solid #ccc;
    border-radius: 4px;
    background-color: #fff;
    max-height: 180px;
    overflow-y: auto;
    z-index: 1000;
    padding: 10px;
    box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);
`;

export const CheckboxItem = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 8px;

    input[type="checkbox"] {
        margin-right: 8px;
    }

    label {
        cursor: pointer;
        color:${({ theme }) => theme.colors.darkGray};
    }
`;

export const ErrorMessage = styled.span`
    color:${({ theme }) => theme.colors.redColor};
    margin-top: 5px;
    display: block;
`;