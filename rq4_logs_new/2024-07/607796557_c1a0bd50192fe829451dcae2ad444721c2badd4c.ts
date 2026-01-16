import styled from 'styled-components';
import { ButtonFilled } from '../../globalStyles';

export const Container = styled.div`
    background-color: #5072eb40;
    border: 1px solid #5072eb99;
    border-radius: 20px;
    padding: 18px;
    position: relative;
    width: 816px;
    height: 560px;

    @media (max-width: 1024px) {
        width: 100%;
        height: 460px;
        max-height: 560px;
    }

    @media (max-width: 768px) {
        width: 100%;
        height: 360px;
        padding: 14px;
        max-height: 370px;
    }

    @media (max-width: 425px) {
        width: 100%;
        height: 300px;
        padding: 8px;
        max-height: 220px;
    }
`;

export const VideoTextAndButton = styled.div`
    font-family: 'Inter', sans-serif;
    position: absolute;
    font-size: 1.5rem;
    line-height: 30px;
    font-weight: 700;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 32px;
    color: #ffffff;
    padding: 0 60px;
    text-align: center;

    @media (max-width: 425px) {
        font-size: 0.875rem;
        gap: 16px;
        line-height: 18px;
    }
`;

export const Button = styled(ButtonFilled)`
    padding: 16px 24px;
    gap: 16px;

    @media (max-width: 425px) {
        font-size: 0.875rem;
        padding: 8px 16px;
    }
`;