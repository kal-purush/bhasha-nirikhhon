import styled from "styled-components";
import { PhoneInputMask } from "../PhoneInput/PhoneInputMask";

export const DivPhone2 = styled.div`
  width: 300px;
  height: 60px;
  border: 2px solid black;
  box-shadow: rgb(1, 1, 1) 20px 20px;
  box-sizing: border-box;
  padding: 0px 20px;
  border-radius: 8px;
  margin: 18px 0px;
  display: flex;
  -webkit-box-align: inherit;
  align-items: inherit;
`;
export const PhoneInput2 = styled(PhoneInputMask)`
  align-self: center;
  width: 300px;
  height: 60px;
  border:none;
  display: flex;
 
`;