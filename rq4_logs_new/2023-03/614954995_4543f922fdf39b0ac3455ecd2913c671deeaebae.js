import styled from 'styled-components';

export const Table = styled.table`
  margin: 0 auto;
  padding: 0;
  width: 800px;

  font-weight: 700;
  font-size: 20px;
  line-height: 1.2;
  text-align: center;
  background-color: #ffffff;

  overflow: hidden;
  border-collapse: collapse;
  border-radius: 10px;
  box-shadow: rgba(99, 99, 99, 0.2) 0px 2px 8px 0px;
`;

export const Theader = styled.thead`
  color: #ffffff;

  background-color: cyan;
`;

export const Tbody = styled.tbody`
  color: gainsboro;
`;

export const Tr = styled.tr`
  padding: 0;

  tbody &:nth-child(2n) {
    background-color: gray;
  }
`;

export const Th = styled.th`
  padding: 10px 0 10px;
`;

export const TdType = styled.td`
  padding: 10px 0 10px;
  padding-left: 15%;
  text-align: left;
`;

export const TdNumber = styled.td`
  padding: 10px 0 10px;
  padding-right: 10%;
  text-align: right;
`;

export const TdCurrency = styled.td`
  padding: 10px 0 10px;
  text-align: center;
`;