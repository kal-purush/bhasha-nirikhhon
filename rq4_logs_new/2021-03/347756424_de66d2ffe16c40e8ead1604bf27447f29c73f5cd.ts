import styled from 'styled-components';

import { FlexContainerColumn } from '../GeneralComponents';

export const Container = styled(FlexContainerColumn)`
    flex: 1;
    justify-content: center;
    align-items: center;

    height: 100vh;
    width: 100vw;

    span {
        text-align: center;
    }

    a {
        text-decoration: underline;
        color: black;
        transition: 0.2s;

        &:hover {
            color: ${({theme}) => theme.color.mediumGray};
            transition: 0.2s;
        }
    }

    @media(max-width: 768px) {
        span {
            font-size: 56px;
        }

        a span {
            font-size: 16px;
        }
    }
`;