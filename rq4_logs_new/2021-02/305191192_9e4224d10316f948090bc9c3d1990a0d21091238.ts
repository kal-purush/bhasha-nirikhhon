import styled from 'styled-components';
import {colors} from '../../../styles/theme';


export const StyledContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    align-items: flex-start;
    grid-gap: 10px;
    grid-template-areas:
        'all all first first'
        'all all first first'
        'all all first first';
    margin: 20px 0;
`;

export const StyledAllContainer = styled.div`
    grid-area: all;
    display: flex;
    flex-direction: column;
    align-items: center;
    box-shadow: 0 4px 12px ${colors.boxShadow};
    height: 72vh;
`;

export const StyledFirstContainer = styled.div`
    grid-area: first;
    display: flex;
    flex-direction: column;
    align-items: center;
    box-shadow: 0 4px 12px ${colors.boxShadow};
    height: 72vh;
`;