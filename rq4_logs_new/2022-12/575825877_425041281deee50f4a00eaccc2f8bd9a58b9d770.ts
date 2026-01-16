import styled from "styled-components";

export const DocumentUploadContainer = styled.div`
  display: flex;
  height: 100%;
  flex: 1;
  justify-content: flex-start;
  width: 100%;
  align-items: flex-start;
  flex-direction: column;
  align-items: center;

  .documentUpload__list {
    list-style: none;
    font-family: "Sansation Light";
    font-style: normal;
    font-weight: 300;
    font-size: 18px;
    line-height: 30px;

    align-items: center;
    letter-spacing: 0.05em;
    color: #000000;

    &--item {
      padding: 10px 0px;
    }

    &--item:before {
      content: "✓";
    }
  }
`;

export const DocumentUploadInformation = styled.p`
  font-family: "Sansation Light";
  font-style: normal;
  font-weight: 300;
  font-size: 17px;
  line-height: 30px;
  display: flex;
  align-items: center;
  text-align: justify;
  letter-spacing: 0.05em;
  color: black;
  padding: 0px 20px;
  margin-top: 50px;
`;