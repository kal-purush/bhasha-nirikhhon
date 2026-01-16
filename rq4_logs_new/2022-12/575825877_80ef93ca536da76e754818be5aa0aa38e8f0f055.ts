import styled from "styled-components";

export const WalletDesktopContainerStyled = styled.form`
  display: flex;
  height: 100%;
  flex: 1;
  justify-content: flex-start;
  margin-top: 30px;
  width: 60%;
  max-width: 800px;
  align-items: flex-start;
  flex-direction: column;
  align-items: center;
  box-shadow: 0px 4px 4px rgba(0, 0, 0, 0.25);
  border-radius: 10px;
  border: 1px solid #ffffff;
  padding: 25px 0px;

  .wallet {
    &__title {
      font-family: "Sansation Light";
      font-style: normal;
      font-weight: 300;
      font-size: 24px;
      line-height: 20px;
      letter-spacing: 0.05em;
      color: #000000;
      margin-bottom: 10px;
    }

    &__subtitle {
      margin-top: 0px;
      font-family: "Sansation Light";
      font-style: normal;
      font-weight: 300;
      font-size: 20px;
      line-height: 45px;
      color: #000000;
    }

    &__layout {
      display: flex;
      flex-direction: column;
      align-items: center;
      width: 90%;
    }
  }
`;

export const WalletDesktopInformationStyled = styled.p`
  font-family: "Sansation Light";
  font-style: normal;
  font-weight: 300;
  font-size: 17px;
  line-height: 30px;
  text-align: left;
  letter-spacing: 0.05em;
  color: black;
  width: 100%;
`;

export const WalletDesktopSelectContainer = styled.div`
  width: 100%;
  height: 100%;
  margin: 20px 0px;
  display: flex;
  flex-direction: column;
  .wallet {
    &__select {
      &__label {
        font-family: "Sansation Light";
        font-style: normal;
        font-weight: 300;
        font-size: 18px;
        line-height: 20px;
        letter-spacing: 0.05em;
        color: #000000;
      }
    }
  }
`;

export const WalletDesktopSelect = styled.select`
  width: 350px;
  height: 51.23px;
  left: 17px;
  top: 278.66px;
  background: #ffffff;
  border: 1px solid ${({ theme }) => theme.colors.mainColor};
  border-radius: 5px;
  font-family: "Sansation Light";
  font-style: normal;
  font-weight: 300;
  font-size: 18px;
  line-height: 20px;
  letter-spacing: 0.05em;
  color: #000000;
  padding-left: 10px;
  margin-top: 10px;
  -moz-appearance: none;
  -webkit-appearance: none;
  appearance: none;
  background: url("img/down-arrow.png") no-repeat;
  background-size: 21px;
  cursor: pointer;
  background-position: right 5px top 50%;
`;

export const WalletDesktopAddressContainer = styled.div`
  width: 100%;
  margin-bottom: 100px;
  .wallet {
    &__address {
      &__label {
        font-family: "Sansation Light";
        font-style: normal;
        font-weight: 300;
        font-size: 18px;
        line-height: 20px;
        letter-spacing: 0.05em;
        color: #000000;
      }
      &__note {
        font-family: "Sansation Light";
        font-style: normal;
        font-weight: 300;
        font-size: 14px;
        line-height: 20px;
        text-align: left;
        letter-spacing: 0.05em;
        margin-left: 10px;
        color: #c4c4c4;
      }
    }
  }
`;

export const WalletDesktopAddress = styled.textarea`
  width: 100%;
  height: 50.23px;
  left: 17px;
  top: 278.66px;
  background: #ffffff;
  border: 1px solid #ff1282;
  border-radius: 5px;
  font-family: "Sansation Light";
  font-style: normal;
  font-weight: 300;
  font-size: 18px;
  line-height: 20px;
  letter-spacing: 0.05em;
  color: #000000;
  padding-left: 10px;
  padding-top: 10px;
  margin-top: 10px;
  -moz-appearance: none;
  -webkit-appearance: none;
  resize: none;
  padding-top: 14px;
`;

export const AgreementsDesktopContainer = styled.div`
  width: 100%;
  font-family: "Sansation Light";
  font-style: normal;
  font-weight: 300;
  font-size: 14px;
  line-height: 20px;
  letter-spacing: 0.05em;
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding-bottom: 30px;
  color: #000000;

  input[type="checkbox"] {
    z-index: 1;
    width: 25px;
    height: 25px;
    opacity: 0;
    cursor: pointer;
  }

  .wallet__terms {
    padding-left: 10px;

    &__container {
      display: flex;
      align-items: center;
      justify-content: flex-start;
    }

    &__custom__checkbox {
      position: absolute;
      height: 25px;
      width: 25px;
      background: #ffffff;
      border: 1px solid #ff1282;
      border-radius: 5px;
    }
    &__accent {
      color: ${({ theme }) => theme.colors.mainColor};
      cursor: pointer;
    }
  }

  .wallet__terms__container input:checked ~ .wallet__terms__custom__checkbox {
    background: url("img/checkmark.svg") no-repeat;
    background-size: 21px;
  }
`;