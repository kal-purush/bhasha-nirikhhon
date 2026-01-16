import styled from "styled-components";

export const Container = styled.div`
  padding: 0 0 16px;
  margin-top: 54px;

  > h3 {
    text-align: center;
    color: #212121;
    margin: 0 -24px 0;
    padding: 8px 0;
    font-weight: 700;

    border: 1px solid #000;
    border-left: none;
    border-right: none;
  }

  .firstRow {
    display: flex;
    margin-top: 40px;

    > div {
      flex: 1;

      span {
        margin-top: 16px;
      }
    }
  }

  .scenarioRow {
    display: flex;
    margin-top: 40px;

    > h6 {
      flex: 1;
    }

    > span,
    div {
      flex: 3;
    }
  }

  .responsibleResourceRow {
    > h6 {
      max-width: 368px;
    }

    > span {
      max-width: 200px;
    }
  }
`;

export const ColumnTitle = styled.h6`
  font-size: 22px;
  line-height: 28px;
  color: #555656;
  font-weight: 700;
`;

export const ColumnValue = styled.span`
  display: block;
  font-size: 20px;
  line-height: 28px;
  color: #555656;
`;