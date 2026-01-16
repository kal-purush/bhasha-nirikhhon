import styled from 'styled-components';

export const Content = styled.div`
  display: flex;
  flex: 1;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

export const TopContentItem = styled.div`
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

export const RankBlock = styled.div`
  margin: 0.5rem 0;
  display: flex;
  flex: 1;
  align-items: center;
`;

export const RankNum = styled.div`
  width: 30px;
  display: flex;
  justify-content: flex-end;
  font-size: 1.5rem;
`;

export const RankPlayer = styled.div`
  border: 1px solid lightgray;
  margin-left: 1rem;
  padding: 0.5rem 1rem;
  width: 360px;
  display: flex;
  flex: 1;
  justify-content: space-between;
  align-items: center;
`;

export const SelectBlock = styled.div`
  border: 1px solid lightgray;
  margin-left: 1rem;
  padding: 0.5rem 1rem;
  width: 360px;
  display: flex;
  &:hover {
    cursor: pointer;
  }
`;

export const DeleteBtn = styled.div`
  &:hover {
    cursor: pointer;
  }
`;