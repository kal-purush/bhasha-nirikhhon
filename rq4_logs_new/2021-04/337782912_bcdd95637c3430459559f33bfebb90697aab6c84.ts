import styled from 'styled-components';

export const StyledWrap = styled.div`
    width: 100%;
    padding: 0 20px;
`;

export const StyledContainer = styled.div`
    max-width: 420px;
    margin: auto;
`;

export const StyledTitle = styled.h2`
    margin: 30px 0 0;
    font-size: 22px;
`;

export const StyledDivider = styled.div`
    box-shadow: inset 0 -1px 0 0 ${({ theme }) => theme.smoke80};
    height: 1px;
    margin-top: 12px;
`;

export const StyledTopHeader = styled.a`
    display: block;
    margin: 42px 0 0;
`;