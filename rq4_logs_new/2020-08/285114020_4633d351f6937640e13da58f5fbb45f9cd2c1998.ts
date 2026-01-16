import styled from 'styled-components';

export const Content = styled.div`
  display: flex;
  flex: 1;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

export const ContentItem = styled.div`
  margin: 1rem 0;
`;

export const PickBlock = styled.div`
  margin: 0.5rem 0;
  display: flex;
  flex: 1;
  align-items: center;
`;

export const PickNum = styled.div`
  width: 30px;
  display: flex;
  justify-content: flex-end;
  font-size: 1.5rem;
`;

export const PickPlayer = styled.div`
  border: 1px solid lightgray;
  margin-left: 1rem;
  padding: 0.5rem 1rem;
  width: 400px;
  display: flex;
  flex: 1;
  justify-content: space-between;
  align-items: center;
`;

export const OwnerName = styled.span`
  margin: 0;
  color: #bada55;
`;

export const DeleteBtn = styled.div`
  &:hover {
    cursor: pointer;
  }
`;