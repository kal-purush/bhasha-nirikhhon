import styled from "styled-components";

export const SectionWrapper = styled.div`
  display: flex;
  flex-direction: column;
  width: 100vw;
  height: 46.875rem;
  padding: 4.75rem 7.5rem;
  gap: 1rem;

  .snr-categories {
    border-bottom: 0.125rem solid #d0d0d0;
    display: flex;
    gap: 2.5rem;

    span {
      border-bottom: 0.25rem solid #fff;
      cursor: pointer;
    }

    span:hover {
      border-bottom: 0.25rem solid #9031d0;
    }
  }
  .snrtop {
    display: flex;
    align-items: center;
    justify-content: space-between;

    h3 {
      display: flex;
      align-items: center;
      font-size: 2.25rem;
      font-weight: 700;
      line-height: 2.75rem; /* 122.222% */
      letter-spacing: -0.045rem;
      gap: 0.5rem;

      .carticon {
        transform: rotateY(180deg);
      }
    }
  }
`;

export const CardsWrapper = styled.div`
  display: flex;
  justify-content: space-between;
  width: 100%;
  margin: 0 auto;
`;

export const Card = styled.div`
  width: 24rem;
  height: 28.125rem;
  flex-shrink: 0;
  border: 1px solid red;
  display: flex;
  flex-direction: column;
  padding: 1.5rem;
  border-radius: 0.5rem;
  border: 1px solid var(--other-colors-bg-color-2, #f4f4f4);
  background: var(--neutral-colors-white, #fff);
  gap: 0.5rem;

  .tagwrapper {
    display: flex;
    gap: 0.625rem;
  }

  .snr-tag {
    display: flex;
    place-items: center;
    border-radius: 0.5rem;
    background: var(--secondary-colors-orange-orange-100, #fcf3ec);
    color: var(--secondary-colors-orange-orange-500, #f29c4a);
    font-size: 0.625rem;
    font-weight: 700;
    line-height: 1.125rem;
    letter-spacing: -0.0125rem;
    padding: 0 0.5rem;
    height: 1.625rem;
  }

  .product-name {
    color: var(--neutral-colors-black-200, #121212);
    font-size: 1.5rem;
    font-weight: 700;
    line-height: 2rem; /* 133.333% */
    letter-spacing: -0.03rem;
  }

  .product-desc {
    color: var(--neutral-colors-grey-200, #a0a0a0);
    font-size: 1rem;
    font-weight: 450;
    line-height: 1.5rem; /* 150% */
    letter-spacing: -0.02rem;
  }

  .price-and-cart {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 2.62rem;

    span {
      color: var(--primary-colors-purple-purple-500, #a456d8);
      font-size: 1.5rem;
      font-weight: 700;
      line-height: 2rem; /* 133.333% */
      letter-spacing: -0.03rem;
    }
  }
`;