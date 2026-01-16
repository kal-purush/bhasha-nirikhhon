import styled from 'styled-components';

type Props = {    
    image: string;
    imageMobile: string;
};

export const Container = styled.div<Props>`
    background-image: url('${ props => props.imageMobile }');
    background-position: center;
    background-repeat: no-repeat;
    background-size: cover;
    width: 150%;
    height: 150%;
    position: absolute;
    top: 0;
    left: 0;
    z-index: -1;

    @media(min-width: 1024px) {
        background-image: url('${ props => props.image }');
    }
`;