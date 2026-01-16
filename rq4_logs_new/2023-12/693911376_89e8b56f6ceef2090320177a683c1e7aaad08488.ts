import { theme } from 'native-base'
import styled from 'styled-components'

export const CategoriesContainer = styled.section`
  position: relative;
  width: 80%;
  height: calc(100vh - 6rem);
  display: flex;
  align-items: center;
  justify-content: space-between;

  .logo-categories-container {
    width: 30%;

    svg {
      width: 17rem;
      height: 19rem;
    }
  }

  .categories-list {
    width: 60%;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    gap: 1rem;

    & > div[role='button'] {
      width: 100%;
    }

    .categories-page-title {
      font-weight: bold;
      font-size: 2rem;
    }
  }

  #create-category {
    width: 100%;
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
  }

  @media (max-width: 930px) {
    .logo-categories-container {
      svg {
        width: 15rem;
        height: 13rem;
      }
    }
  }

  @media (max-width: 950px) {
    .logo-categories-container {
      display: none;
    }

    .categories-list {
      width: 100%;
    }
  }
`

export const CategoriesListContainer = styled.div`
  width: 100%;
  height: 70vh;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  grid-template-rows: auto;
  align-items: center;
  justify-items: center;
  align-content: space-between;
  gap: 1rem;

  overflow-y: auto;
  padding: 0 0 1rem 0;

  #category-card {
    position: relative;
    width: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;

    /* grid-column: 1 / span 2;
    grid-row: 1 / 3; */

    padding: 1rem;
    background-color: ${theme.colors.muted[100]};
    border: 1px solid ${theme.colors.black + '70'};
    border-radius: 8px;
    box-shadow: 0 0 5px ${theme.colors.black + '50'};

    #category-name {
      margin: 1rem 0 0 0;
      font-size: 1rem;
      font-weight: bold;
      margin-bottom: 0.5rem;
      color: ${theme.colors.primary[700]};
    }
  }

  @media (max-width: 768px) {
    grid-template-columns: repeat(1, 1fr);
  }
`

export const ColorIndicator = styled.span<{ colorIndicator: string }>`
  width: 50px;
  height: 20px;
  border-radius: 4px;
  /* Use a placeholder color property; you may replace it with your logic */
  background-color: ${(props) => props.colorIndicator}; /* Example color */
  /* You can adjust the size and other properties as needed */
`

export const EditButtonContainer = styled.button`
  position: absolute;
  top: 0;
  right: 0;

  display: flex;
  align-items: center;
  justify-content: center;

  padding: 0.5rem;
  border-radius: 0 8px 0 0;
  transition: 0.3s all ease;

  background-color: ${theme.colors.emerald['500']};

  &:hover {
    filter: brightness(70%);
  }

  @media (max-width: 768px) {
    padding: 0.5rem;
  }
`