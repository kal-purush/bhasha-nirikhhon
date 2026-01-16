import styled from 'styled-components';

export const CardContainer = styled.div`
    width: 100%;
    @media (min-width: 768px) {
        width: 30%;
    }
    margin: 25px 0;
    position: relative;
    border-radius: 0.5rem;
`;

export const CardHeader = styled.div`
    position: relative;
    overflow: hidden;
`;

export const CardImage = styled.div`
    width: 100%;
    background: #202329;
`;

export const CardTitle = styled.div`
    width: 100%;
    opacity: 0.8;
    position: absolute;
    bottom: 0px;
    background: rgb(32, 35, 41);
    padding: 0.625rem;
`;

export const CardName = styled.h2`
    font-weight: bold;
    color: rgb(245, 245, 245);
    margin: 0;
`;

export const CardDesc = styled.p`
    color: rgb(187, 187, 187);
    margin: 0;
`;

export const CardBody = styled.div`
    color: rgb(255, 152, 0);
    padding: 10px 1rem;
    background: rgb(51, 51, 51);
    height: 200px
`;

export const TextWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 10px 0;

    span {
        font-size: 0.7rem;
        color: #9e9e9e;
    }
    p {
        width: 100%;
        font-size: 0.9rem;
        font-weight: 200;
        text-align: right;
        margin: 0;
    }
`;

export default {
    CardImage,
    CardTitle,
    CardContainer,
    CardName,
    CardDesc,
    CardBody,
    TextWrapper,
    CardHeader,
};