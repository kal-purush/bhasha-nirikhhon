import styled from 'styled-components';

export const Content = styled.div`
  display: flex;
  flex: 1;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

export const RowContent = styled.div`
  display: flex;
  flex: 1;
  justify-content: space-between;
  align-items: center;
`;

export const ContentItem = styled.div`
  margin: 1rem 0;
`;

export const ScoringTypeBlock = styled.div`
  margin-right: 4rem;
  display: flex;
  flex: 1;
  justify-content: space-between;
  align-items: center;
`;

export const RadioBlock = styled.div`
  display: flex;
  flex: 1;
  align-items: center;
  margin: 0 1rem;
`;

export const DeleteBtn = styled.div`
  &:hover {
    cursor: pointer;
  }
`;