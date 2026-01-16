import styled from 'styled-components';

export const Container = styled.div`
  padding: 3rem 2rem;
`;
export const Header = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;

  > .title {
    font-style: normal;
    font-weight: 500;
    font-size: 2.4rem;
    line-height: 2.8rem;
    color: ${({ theme }) => theme.colors.textColor};
  }
`;
export const Filter = styled.div`
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 19rem;
  height: 4.4rem;
  padding: 0.8rem;

  background: rgba(196, 196, 196, 0.2);
  border-radius: 0.4rem;

  > select {
    cursor: pointer;
    flex: 1;
    height: 100%;
    border: 0;
    background-color: transparent;
    text-align: center; /* para firefox */
    text-align-last: center; /* para chrome */
    font-size: 2rem;
    color: ${({ theme }) => theme.colors.textColor};
    -webkit-appearance: none;
    -moz-appearance: none;
    text-indent: 1px;
    text-overflow: '';
  }
`;

export const ListSalesContainer = styled.div`
  border: 1px solid #000;
`;
export const LabelContainer = styled.div`
  border: 1px solid red;
`;
export const LabelItems = styled.div``;
export const SalesContainer = styled.div`
  border: 1px solid green;
`;
export const Item = styled.div``;