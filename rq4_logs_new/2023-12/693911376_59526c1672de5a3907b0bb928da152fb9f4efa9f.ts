import styled from 'styled-components'

export const SavedTopicsContainer = styled.section`
  position: relative;
  width: 80%;
  height: calc(100vh - 6rem);
  display: flex;
  flex-direction: row;
  align-items: center;
  /* justify-content: center; */

  .logo-saved-topics-container {
    width: 30%;

    svg {
      width: 17rem;
      height: 19rem;
    }
  }

  .saved-topics-list {
    width: 60%;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    gap: 1rem;
  }

  .saved-topics-title {
    font-weight: bold;
    font-size: 2rem;
  }

  & > div[role='button'] {
    width: 30%;
  }

  .reload-saved-topics {
    width: 100%;
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
  }

  @media (max-width: 930px) {
    .logo-saved-topics-container {
      svg {
        width: 15rem;
        height: 13rem;
      }
    }

    .reload-saved-topics {
      flex-direction: column;
      gap: 0.7rem;

      & > div[role='button'] {
        width: 100%;
      }
    }
  }

  @media (max-width: 950px) {
    .logo-saved-topics-container {
      display: none;
    }

    .saved-topics-list {
      width: 100%;
    }

    .saved-topics-title {
      font-weight: bold;
      font-size: 1.5rem;
    }
  }
`

export const SavedTopicsListContainer = styled.div`
  width: 100%;
  max-height: 70vh;
  display: flex;
  flex-direction: column;
  grid-template-rows: auto;
  align-items: center;
  justify-items: center;
  align-content: space-between;
  gap: 1rem;

  overflow-y: auto;
  padding: 0 0 1rem 0;

  @media (max-width: 768px) {
    grid-template-columns: repeat(1, 1fr);
  }
`