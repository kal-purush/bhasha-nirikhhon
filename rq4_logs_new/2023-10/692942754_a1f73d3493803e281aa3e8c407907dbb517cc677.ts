import styled, { css } from "styled-components";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

export const Container = styled.div`
  position: fixed;
  bottom: 0;
  width: 380px;
  margin: 16px;
  display: flex;
  flex-direction: column;
`;

export const Message = styled.div`
  width: 100%;
  min-height: 44px;
  border: 1px solid #ccc;
  border-radius: 20px;
  background-color: #fff;

  ${({ filesSelected }) =>
    filesSelected
      ? css`
          display: flex;
          flex-direction: row;
        `
      : css`
          display: flex;
          flex-direction: column;
        `};
`;

export const Input = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  width: 100%;
  min-height: 44px;
  padding: 0 16px 0 11px;
`;

export const TextInput = styled.input`
  display: flex;
  padding-left: 10px;
  border: none;
  outline: none;
  font-size: 16px;
  color: #222;
  background-color: transparent;
`;

export const ImageInput = styled.input`
  min-height: 100px;
  display: none;
`;

export const DirectBtn = styled.div`
  display: flex;
  flex-direction: row;
`;

export const UploadBtn = styled.button`
  border: none;
  outline: none;
  background-color: transparent;
`;

export const SendBtn = styled.button`
  border: none;
  outline: none;
  margin-left: 3px;
  font-size: 14px;
  color: #0095f6;
  background-color: transparent;
`;

export const ImageIcon = styled(FontAwesomeIcon)`
  padding: 10px;
`;

export const PreviewContainer = styled.div`
  display: flex;
  flex-direction: row;
  border-top-left-radius: 20px;
  border-top-right-radius: 20px;
  width: 100%;
  padding: 12px;
  background-color: #fff;
`;

export const ImagePreview = styled.div`
  position: relative;
  display: flex;
  flex-direction: row;
  align-items: center;
  margin-right: 12px;
`;

export const Image = styled.img`
  width: 48px;
  height: 48px;
  border-radius: 5px;
`;

export const CloseIcon = styled(FontAwesomeIcon)`
  position: absolute;
  top: -10px;
  right: -10px;
`;