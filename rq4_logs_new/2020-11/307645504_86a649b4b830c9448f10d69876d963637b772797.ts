import styled from 'styled-components';

import media from '../../modules/media';
export const Menu = styled.ul`
  ${(props) => props.hide && 'display: none;'}
`;

export const MenuItem1 = styled.li`
  font-size: 16px;
  padding: 20px;
  display: flex;
  flex-direction: column;
  border-bottom: 2px solid #42535b;
  background-color: #36474f;

  a {
    color: white;
    &:link {
      text-decoration: none;
    }
  }

  ${(props) => props.url === '' && 'border-left: 3px solid #add8e6;'}

  ${media.tablet} {
    text-align: center;
    a {
      padding: 20px;
      display: block;
    }
  }
`;

export const MenuItem2 = styled.li`
  font-size: 16px;
  padding: 20px;
  display: flex;
  flex-direction: column;

  border-bottom: 2px solid #42535b;
  background-color: #36474f;

  ${(props) =>
    props.url === 'addCommunity' && 'border-left: 3px solid #add8e6;'}

  ${media.tablet} {
    text-align: center;
    a {
      padding: 20px;
      display: block;
    }
  }
  a {
    color: white;
    &:link {
      text-decoration: none;
    }
  }
`;

export const Wrapper = styled.div`
  display: flex;
  list-style-type: none;
  height: 100%;

  position: fixed;
  ${(props) => !props.hide && 'background-color: #36474f;'}
  flex-direction: column;
  width: 20%;
  top: 0;
  left: 0;
  margin: 0px 800px 0px 0px;
  padding: 0;

  ${media.tablet} {
    flex-direction: row;

    width: 100%;
    height: 30%;
    top: 0;
    left: 0px;
    margin: 0px;
    padding: 0;
  }
`;