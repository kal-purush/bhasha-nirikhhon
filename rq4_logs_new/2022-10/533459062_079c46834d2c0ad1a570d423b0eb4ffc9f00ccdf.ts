import styled from 'styled-components';


export const StyledUserPanel = styled.div`
  position: relative;
  display: inline-block;
`;

export const StyledUserPanelOverlay = styled.div<{ isUserPanelExtended: boolean }>`
  position: fixed;
  display: ${props => props.isUserPanelExtended ? 'block' : 'none'};
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  z-index: 2;
`;


export const StyledUserPanelDropdownContainer = styled.div`
  position: absolute;
  color: ${props => props.theme.colors.black};
  width: 100%;
  left: 0;
  right: 0;
  z-index: 3;
`;

export const StyledUserPanelDropdownList = styled.ul`
  position: absolute;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
  margin: 0;
  padding: 0;
  width: 214px;
  background-color: ${props => props.theme.colors.white};
  filter: drop-shadow(0px 4px 35px rgba(168, 172, 176, 0.2));

`;

export const StyledUserPanelDropdownItem = styled.li`
  list-style-type: none;
  width: 100%;
  display: flex;
  flex-direction: column;
  position: relative;
`;

export const StyledHr = styled.hr`
  width: 100%;
  height: 1px;
  border: none;
  background-color: ${props => props.theme.colors.gray};
`;

export const StyledNote = styled.p`
  display: inline-block;
  position: absolute;
  padding: 0 5px;
  font-size: 10px;
  background-color: ${props => props.theme.colors.white};
  color: ${props => props.theme.colors.darkGray};
  align-self: center;
`;

export const StyledUserPanelDropdownSignInButton = styled.button`
  margin: 15px;
  align-self: center;
  font-size: 18px;
  line-height: 250%;
  width: 85%;
  text-align: center;
  text-indent: 10px;
  background-color: ${props => props.theme.colors.orange};
  border: 2px solid ${props => props.theme.colors.orange};
  border-radius: 10px;

  &:hover {
    background-color: ${props => props.theme.colors.red};
    border: 2px solid ${props => props.theme.colors.red};

  }
`;

export const StyledUserPanelDropdownSignUpButton = styled.button`
  margin: 5px;
  align-self: center;
  font-size: 18px;
  line-height: 250%;
  width: 85%;
  text-align: center;
  text-indent: 10px;
  background-color: ${props => props.theme.colors.white};
  border: 2px solid ${props => props.theme.colors.orange};
  border-radius: 10px;


  &:hover {
    background-color: ${props => props.theme.colors.red};
    border: 2px solid ${props => props.theme.colors.red};

  }
`;

export const StyledUserPanelDropdownItemButton = styled.button`
  font-size: 18px;
  line-height: 250%;
  width: 100%;
  text-indent: 10px;
  //align-items: center;
  //justify-content: center;

  &:hover {
    background-color: #EEEEEE;
  }
`;

export const StyledUserPanelHeader = styled.button`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  padding: 0 10px;
  gap: 10px;
  width: 32px;
  height: 29px;
`;

export const StyledUserPanelSymbol = styled.div`
  font-weight: 500;
  text-align: right;
  font-size: 18px;
`;

export const StyledIcon = styled.img<{ isUserPanelExtended?: boolean }>`
  width: 25px;
  height: 25px;
  //filter: ${props => props.isUserPanelExtended && 'invert(0%) sepia(95%) saturate(21%) hue-rotate(291deg) brightness(98%) contrast(101%)'};
  filter: invert(15%) sepia(10%) saturate(1962%) hue-rotate(197deg) brightness(94%) contrast(90%);
`;

export const StyledOptionsIcon = styled.img<{ isUserPanelExtended?: boolean }>`
  width: 25px;
  height: 25px;
  vertical-align: text-bottom;
  filter: invert(15%) sepia(10%) saturate(1962%) hue-rotate(197deg) brightness(94%) contrast(90%);
`;

export const StyledAccountButtonText = styled.p`
  padding-left: 15px;
 color: ${props => props.theme.colors.black};
`;