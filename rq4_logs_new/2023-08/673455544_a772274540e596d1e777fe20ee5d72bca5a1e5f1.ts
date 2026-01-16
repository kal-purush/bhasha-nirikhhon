import { styled } from "styled-components";

export const ModalContainer = styled.div`
  background-color: rgba(0, 0, 0, 0.5);
  width: 100%;
  height: 100vh;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 10;
  display: flex;
  justify-content: center;
  align-items: center;
`;

export const Modal = styled.form`
  width: 90%;
  height: 28vh;
  background-color: #fff;
  border-radius: 15px;
  padding: 20px;
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  position: relative;
  gap: 20px;

  .input-modal-comments {
    width: 100%;
    height: 35vh;
  }

  @media (min-width: 768px) {
    width: 40%;
  }
`;

export const DivTitle = styled.div`
  display: flex;
  justify-content: flex-start;
  align-items: center;

  h2 {
    margin: 10px 0;
    font-size: 16px;
    color: #212529;

    @media (min-width: 768px) {
      font-size: 20px;
    }
  }

  svg.close-btn-modalComments {
    position: absolute;
    right: 4%;
    font-size: 20px;
    cursor: pointer;
    color: #adb5bd;
    position: absolute;

    @media (min-width: 768px) {
      right: 3%;
    }
  }
`;

export const DivButtons = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  width: 97%;
  gap: 30px;
`;