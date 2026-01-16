import { styled } from "styled-components";
import { GameProductCardProps } from "./GameProductCard";

export const GameProductsWrapper = styled.section`
  display: flex;
`;

export const GameProductCardWrapper = styled.div<GameProductCardProps>`
  background: ${({ $bgColor }) => $bgColor};
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 3rem 3.75rem 3.75rem 3.75rem;
  gap: 2.5rem;
  cursor: pointer;

  p {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 1.25rem;
    font-weight: 450;
    line-height: 1.75rem; /* 140% */
    letter-spacing: -0.025rem;
  }

  .card-details {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
`;

export const ProductImageWrapper = styled.div<GameProductCardProps>`
  background: ${({ $bgImg }) => $bgImg} no-repeat center;
  background-size: 100% 100%;
  width: 22.5rem;
  height: 28.125rem;
  background: #fff;
`;

export const ProductTitle = styled.span<GameProductCardProps>`
  color: ${({ $color }) => $color};
  font-size: 1.75rem;
  font-weight: 700;
  line-height: 2.25rem; /* 128.571% */
  letter-spacing: -0.035rem;
`;

export const Buttons = styled.div`
  display: flex;
  gap: 1.5rem;
  margin-top: 1rem;

  button {
    border: 1px solid red;
    display: flex;
    flex-direction: row-reverse;
    align-items: center;
    gap: 0.5rem;
  }
`;

export const SectionTitle = styled.h1`
  color: var(--neutral-colors-black-200, #121212);
  text-align: center;
  font-size: 2.25rem;
  font-weight: 700;
  line-height: 2.75rem; /* 122.222% */
  letter-spacing: -0.045rem;
`;