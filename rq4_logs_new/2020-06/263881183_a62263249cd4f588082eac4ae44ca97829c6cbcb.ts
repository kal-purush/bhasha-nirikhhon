import styled from '../../styles/theme';
import { navBar } from '../../styles/heights';

// TODO tidy this to use vars from theme
export const styledNavBar = (NavBar: React.ComponentType) => styled(NavBar)`
  position: relative;
  display: flex;
  top: 0;
  left: 0;
  right: 0;
  padding: 0;
  height: ${navBar};
  box-shadow: 4px 0 12px -6px gray, -4px 0 12px -6px gray;
  list-style-type: none;

  ul {
    display: flex;
    align-items: center;
    position: relative;
    width: 100%;
    margin: 0;
    padding-left: 40px;
    li {
      list-style-type: none;
      padding: 0 4px;
      height: 100%;
      display: flex;
      justify-content: center;
      align-items: center;
    }
  }
`;

export const styledProfile = (Profile: React.ComponentType) =>
  styled(Profile)`
    position: absolute;
    cursor: pointer;
    right: 40px;
    div {
      display: flex;
      align-items: center;
    }
    .details {
      align-items: flex-start;
      flex-direction: column;
    }
    img {
      padding-right: 8px;
      width: 50px;
      border-radius: 50%;
    }
    &:hover {
      background-color: #f1f1f1;
    }
  `;