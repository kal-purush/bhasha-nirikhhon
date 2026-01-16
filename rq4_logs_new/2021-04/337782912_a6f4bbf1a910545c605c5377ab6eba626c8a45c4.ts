import styled from 'styled-components';
import lighten from 'polished/lib/color/lighten';

export const StyledTitle = styled.h2`
    margin: 50px 0 24px;
`;

export const StyledDivider = styled.div`
    border-bottom: 1px solid ${({ theme }) => theme.smoke80};
    margin: 10px 0 6px;
`;

export const StyledFieldSet = styled.fieldset`
    border: 0;
    padding: 0;
`;

export const StyledLabel = styled.label`
    display: block;
    margin: 14px 0 4px;
    font-size: 15px;
    font-weight: bold;
    color: ${({ theme }) => theme.black80};
    span {
        margin-left: 4px;
        font-size: 12px;
        font-weight: 400;
        color: ${({ theme }) => theme.pink};
    }
`;

export const StyledInput = styled.input`
    width: 100%;
    background-color: ${({ theme }) => theme.smoke50};
    font-size: 14px;
    padding: 14px 10px;
    border-radius: 8px;
    border: 1px solid ${({ theme }) => theme.smoke50};
    transition: background-color ease 0.3s, border ease 0.1s;
    &:hover,
    &:focus {
        background-color: ${({ theme }) => theme.smoke1};
    }
    &:focus {
        border: 1px solid ${({ theme }) => lighten(0.2, theme.pink)};
    }
`;

export const StyledSubmitButton = styled.input`
    width: 100%;
    background-color: ${({ theme }) => theme.pink};
    color: white;
    padding: 10px 0;
    border-radius: 8px;
    margin-top: 20px;
    font-weight: 500;
    cursor: pointer;
`;

export const StyledJoinMember = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${({ theme }) => theme.black80};
    text-align: center;
    margin: 20px 0 0 0;
    button {
        margin-left: 4px;
        color: ${({ theme }) => theme.primary100};
        cursor: pointer;
    }
`;