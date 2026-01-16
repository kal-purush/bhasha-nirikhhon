import { FaBars } from "react-icons/fa";
import { MdKeyboardArrowLeft } from "react-icons/md";
import { Link } from "react-router-dom";
import styled from "styled-components";

interface SidebarProps {
    $isOpen: boolean;
}

interface TextProps {
    $isOpen: boolean;
}

export const SidebarContainer = styled.div<SidebarProps>`
  width: ${({ $isOpen }) => ($isOpen ? '269px' : '60px')};
  height: calc(100vh - 70px);
  background-color: ${({ theme }) => theme.colors.primary};
  color: ${({ theme }) => theme.colors.whiteColor};
  display: flex;
  flex-direction: column;
  align-items: center;
  transition: width 0.4s;
`;

export const SidebarHeader = styled.div`
  width: 100%;
  padding: 20px;
  display: flex;
  justify-content: space-between;
  align-items: center;

  h3{
    font-size:1.5rem;
    font-weight:400;
    color:${({ theme }) => theme.colors.textColorSideBar};
  }
`;

export const LineDivisor = styled.div<SidebarProps>`
    display:flex;
    justify-content:center;
    width:100%;
    height:1px;

    hr{
        width:${({ $isOpen }) => ($isOpen ? '80%' : '40%')};
        height:1px;
        margin-left:12px;
        border: 1px solid black;
        opacity:20%;
    }
`

export const IconContainer = styled.div`
    display:flex;
    justify-content:center;
    align-items:center;
`

export const MenuIcon = styled(FaBars)`
  font-size: 24px;
  cursor: pointer;
  color:${({ theme }) => theme.colors.textColorSideBar};
`;

export const MenuArrow = styled(MdKeyboardArrowLeft)`
  font-size: 24px;
  cursor: pointer;
  color:${({ theme }) => theme.colors.textColorSideBar};
`

export const NavMenu = styled.ul`
  width: 100%;
  list-style: none;
  padding: 0;
  margin-top: 20px;
  display:flex;
  flex-direction:column;
  gap:14px;
`;

export const NavItem = styled(Link)`
  width: 100%;
  padding: 10px 20px;
  display: flex;
  align-items: center;
  cursor: pointer;
  transition: all 0.3s ease;
  text-decoration:none;

  &:hover {
    background-color: ${({ theme }) => theme.colors.backgroundColor};
    transform: translateY(-2px);
    border-radius:14px 0px 0px 14px;
    margin-left:10px;


    div, li{
        color:${({ theme }) => theme.colors.textColorSideBarHover};
    }
  }

`;

export const Icon = styled.div`
  font-size: 20px;
  color:${({ theme }) => theme.colors.textColorSideBar};
`;

export const Text = styled.li<TextProps>`
  margin-left: 20px;
  display: ${({ $isOpen }) => ($isOpen ? 'inline' : 'none')};
  color:${({ theme }) => theme.colors.textColorSideBar};
  font-size:1.2rem;

  &:hover {
    color:${({ theme }) => theme.colors.textColorSideBarHover};
  }
`;

export const UserContainer = styled.div`
  margin-top: auto;
  width: 100%;
  padding: 10px;
  display: flex;
  align-items: center;
  background-color: ${({ theme }) => theme.colors.primary};
`;

export const UserImage = styled.img`
  width: 40px;
  height: 40px;
  border-radius: 50%;
`;

export const UserName = styled.span<TextProps>`
  margin-left: 10px;
  display: ${({ $isOpen }) => ($isOpen ? 'inline' : 'none')};
`;