import styled from "styled-components";
import { BlogCardStyledProps } from "./BlogSectionData";

export const BlogCardWrapper = styled.div`
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  width: 24rem;
  height: 31.25rem;
  flex-shrink: 0;
  padding: 1.5rem;
  border-radius: 0.5rem;
  border: 1px solid var(--other-colors-bg-color-2, #f4f4f4);
  background: var(--neutral-colors-white, #fff);
`;

export const CardTop = styled.div`
  display: flex;
  gap: 0.75rem;
  align-items: center;
`;

export const AvatarWrapper = styled.div<BlogCardStyledProps>`
  background: url("${({ $avatar }) => $avatar}") no-repeat center;
  background-size: 100% 100%;
  width: 2.5rem;
  height: 2.5rem;
  border-radius: 50%;
  flex-shrink: 0;
`;

export const NameDuration = styled.div`
  display: flex;
  flex-direction: column;

  .name {
    color: #000;
    font-size: 1rem;
    font-weight: 450;
    line-height: 1.5rem; /* 150% */
    letter-spacing: -0.02rem;
  }

  .duration {
    color: var(--neutral-colors-grey-200, #a0a0a0);
    font-size: 0.875rem;
    font-weight: 450;
    line-height: 1.375rem; /* 157.143% */
    letter-spacing: -0.0175rem;
  }
`;

export const AticleImageWrapper = styled.div<BlogCardStyledProps>`
  background: url("${({ $articleImage }) => $articleImage}") no-repeat center;
  background-size: 100% 100%;
  width: 21rem;
  height: 16.25rem;
  flex-shrink: 0;
  border-radius: 0.5rem;
`;

export const AticleBottom = styled.div`
  button {
    font-size: 0.625rem;
    font-weight: 700;
    line-height: 1.125rem; /* 180% */
    letter-spacing: -0.0125rem;
    display: flex;
    padding: 0.25rem 0.75rem;
    align-items: flex-start;
    gap: 0.625rem;
    border-radius: 0.5rem;
  }

  p {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 1.5rem;
    font-weight: 700;
    line-height: 2rem; /* 133.333% */
    letter-spacing: -0.03rem;
  }
`;

export const BlogSectionWrapper = styled.div`
  display: flex;
  flex-direction: column;
  padding: 4.75rem 7.5rem 6.12rem 7.5rem;
  gap: 2rem;
`;

export const BlogSectionTop = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;

  h1 {
    display: flex;
    gap: 1rem;
    color: #000;
    font-size: 2.25rem;
    font-weight: 700;
    line-height: 2.75rem; /* 122.222% */
    letter-spacing: -0.045rem;

    span {
      color: var(--neutral-colors-grey-200, #a0a0a0);
    }
  }
`;

export const BlogSectionCards = styled.div`
  display: flex;
  gap: 1.5rem;
  margin: 0 auto;
`;