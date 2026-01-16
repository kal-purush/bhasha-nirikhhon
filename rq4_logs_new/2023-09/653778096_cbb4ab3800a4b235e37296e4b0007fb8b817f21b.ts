import styled from "styled-components";

export const GameExperienceWrapper = styled.div`
  display: flex;
  flex-direction: column;
  margin-top: 5rem;
  padding: 5rem 7.5rem;
  align-items: center;
  gap: 1.5rem;
  position: relative;
  z-index: 2;

  h1 {
    color: var(--neutral-colors-black-200, #121212);
    text-align: center;
    font-size: 4rem;
    font-weight: 450;
    line-height: 4.5rem; /* 112.5% */
    letter-spacing: -0.08rem;

    span {
      font-weight: 700;
    }
  }

  p {
    color: var(--neutral-colors-black-200, #121212);
    text-align: center;
    font-size: 1.5rem;
    font-weight: 450;
    line-height: 2rem; /* 133.333% */
    letter-spacing: -0.03rem;
  }

  img {
    position: absolute;
    z-index: -1;
  }

  .plus1 {
    left: 67.2rem;
    top: 0;
  }

  .plus2 {
    top: 0;
    left: -13rem;
  }
`;

export const SelectWrapper = styled.div`
  display: inline-flex;
  padding: 1.5rem 2.5rem;
  align-items: center;
  gap: 1.5rem;
  border-radius: 1.5rem;
  border: 1px solid var(--other-colors-bg-color-2, #f4f4f4);
  background: #fff;

  select {
    color: #000;
    text-align: center;
    font-size: 1.25rem;
    font-weight: 450;
    line-height: 1.75rem; /* 140% */
    letter-spacing: -0.025rem;
  }
  .visible-border {
    border-right: 0.125rem solid #d0d0d0;
    padding-right: 1.5rem;
    display: flex;
  }
`;

export const FirstCardWrapper = styled.div<{ $outerBg: string }>`
  background: ${({ $outerBg }) => $outerBg};
  width: 62.5rem;
  height: 21.625rem;
  flex-shrink: 0;
  border-radius: 2rem;
  display: flex;
  align-items: center;
  padding: 3rem;
  gap: 3rem;
`;

export const FirstCardRight = styled.div<{ $innerBg: string }>`
  background: ${({ $innerBg }) => $innerBg};
  width: 20.75rem;
  height: 15.625rem;
  flex-shrink: 0;
  border-radius: 1.25rem;
`;

export const FirstCardLeft = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 1rem;
  color: var(--neutral-colors-white, #fff);

  h1 {
    font-size: 3rem;
    font-weight: 700;
    line-height: 3.5rem; /* 116.667% */
    letter-spacing: -0.06rem;
    text-align: left;
  }

  p {
    font-size: 1.5rem;
    font-weight: 450;
    line-height: 2rem; /* 133.333% */
    letter-spacing: -0.03rem;
    text-align: left;
  }
`;

export const CardsAndSelectors = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 1.5rem;
`;

export const Selector = styled.div<{
  $bgColor: string;
  $width: string;
  $cursor: string;
}>`
  border-radius: 0.25rem;
  background: ${({ $bgColor }) => $bgColor};
  width: ${({ $width }) => $width};
  height: 0.5rem;
  cursor: ${({ $cursor }) => $cursor};
`;

export const Selectors = styled.div`
  display: flex;
  gap: 0.5rem;
`;

export const CollectionsWrapper = styled.div`
  display: flex;
  flex-direction: column;
  margin-top: 5rem;
  gap: 2.5rem;

  h1 {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 3rem;
    font-weight: 700;
    line-height: 3.5rem; /* 116.667% */
    letter-spacing: -0.06rem;
    text-align: left;

    .span1 {
      color: var(--primary-colors-pink-pink-600, #ff4680);
    }

    .span2 {
      color: var(--primary-colors-purple-purple-600, #9031d0);
    }
  }
`;

export const CollectionsContent = styled.div`
  display: flex;
  gap: 1rem;
  width: 100%;
`;

export const AllCollections = styled.div`
  width: 17.625rem;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  //   overflow-y: scroll;

  .title {
    display: flex;
    padding: 0.75rem 1rem;
    align-items: center;
    border-radius: 0.75rem;
    background: var(--other-colors-bg-color-2, #f4f4f4);
    color: #000;
    font-size: 1.125rem;
    font-weight: 700;
    line-height: 1.625rem; /* 144.444% */
    letter-spacing: -0.0225rem;
    position: absolute;
    z-index: 2;
    width: 17.625rem;
  }
`;

export const CollectionsList = styled.ul`
  display: flex;
  flex-direction: column;
  height: 38.62rem;
  overflow-y: scroll;
  margin-top: 3rem;

  &::-webkit-scrollbar {
    width: 0.3125rem;
    border-radius: 0.625rem;
    background: var(--other-colors-bg-color-1, #fafafa);
  }

  &::-webkit-scrollbar-thumb {
    border-radius: 0.625rem;
    background: var(--primary-colors-purple-purple-500, #a456d8);
    min-height: 40px;
  }

  li {
    text-align: left;
    padding: 0.75rem 1rem;
    color: var(--neutral-colors-grey-200, #a0a0a0);
    font-size: 1.125rem;
    font-weight: 450;
    line-height: 1.625rem; /* 144.444% */
    letter-spacing: -0.0225rem;
  }
`;

export const Experiences = styled.div`
  width: 75%;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
`;

export const ExperiencesTop = styled.div`
  display: flex;
  align-items: center;
  width: 100%;
  gap: 0.2rem;
  position: relative;

  input {
    padding: 0.75rem 3.25rem;
    border-radius: 0.75rem;
    border: 1px solid #ebebeb;
    background: var(--other-colors-bg-color-2, #f4f4f4);
    width: 36.9375rem;

    &:focus {
      outline: none;
    }
  }

  span {
    margin-left: auto;
  }

  .search-icon {
    position: absolute;
    width: 1.5rem;
    height: 1.5rem;
    left: 1rem;
    color: #717171;
  }
`;

export const SecondCardWrapper = styled.div`
  display: flex;
  flex-direction: column;
  gap: 1rem;
  width: 17.625rem;

  h3 {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 1.25rem;
    font-weight: 700;
    line-height: 1.75rem; /* 140% */
    letter-spacing: -0.025rem;
  }

  .cost-p {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 1.125rem;
    font-weight: 450;
    line-height: 1.5rem; /* 150% */
    letter-spacing: -0.02rem;
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .cost {
    display: flex;
    padding: 0.25rem 0.75rem;
    align-items: center;
    border-radius: 0.5rem;
    background: var(--secondary-colors-green-green-100, #eef8f1);
    color: var(--secondary-colors-green-green-500, #32c687);
    font-weight: 450;
    line-height: 1.625rem; /* 144.444% */
    letter-spacing: -0.0225rem;
  }

  .desc {
    color: var(--neutral-colors-grey-300, #717171);
    font-size: 1rem;
    font-weight: 450;
    line-height: 1.5rem; /* 150% */
    letter-spacing: -0.02rem;
    text-align: left;
  }
`;

export const SecondCardImage = styled.div<{ $bgImg: string }>`
  background: url("${({ $bgImg }) => $bgImg}") no-repeat center;
  background-size: 100% 100%;
  width: 100%;
  height: 12.5rem;
  flex-shrink: 0;
  display: flex;
  padding: 1rem;
`;

export const Virtual = styled.div`
  display: inline-flex;
  padding: 0.125rem 0.375rem;
  align-items: center;
  gap: 0.25rem;
  border-radius: 0.25rem;
  background: var(--neutral-colors-white, #fff);
  margin-left: auto;
  height: 1.5rem;

  span {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 0.875rem;
    font-weight: 500;
    line-height: 1.25rem; /* 142.857% */
    letter-spacing: -0.0175rem;
  }
`;

export const CardBottom = styled.div`
  display: flex;
  gap: 1.25rem;

  span {
    display: flex;
    padding: 0.125rem 0.375rem;
    align-items: flex-start;
    border-radius: 0.25rem;
    background: var(--other-colors-bg-color-2, #f4f4f4);
    color: var(--neutral-colors-grey-300, #717171);
    font-size: 0.875rem;
    font-weight: 450;
    line-height: 1.375rem; /* 157.143% */
    letter-spacing: -0.0175rem;
  }
`;

export const ExperiencesCards = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  height: 38.62rem;
  overflow-y: scroll;
  gap: 1.62rem 0;

  &::-webkit-scrollbar {
    width: 0.3125rem;
    border-radius: 0.625rem;
    background: var(--other-colors-bg-color-1, #fafafa);
  }

  &::-webkit-scrollbar-thumb {
    border-radius: 0.625rem;
    background: var(--primary-colors-purple-purple-500, #a456d8);
    min-height: 40px;
  }
`;