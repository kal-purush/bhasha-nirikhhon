import styled from 'styled-components'

interface Props {
  open: boolean
}

export const StyledMenu = styled.nav<Props>`
  display: none;
  flex-flow: column wrap;
  background-color: #ffc674;
  opacity: 90%;
  position: fixed;
  transform: ${({ open }) => open ? 'translateY(0%)' : 'translateY(-102%)'};
  top: 0;
  right: 0;
  height: 70vh; 
  list-style: none;
  display: flex;
  width: 100%;
  padding-top: 3.5rem;
  transition: transform 0.3s ease-in-out;
  align-items: center;
  overflow: hidden;
`;

export const DownwardTriangle = styled.div`
  margin: 30% 0 -30% -100%;
  width: 200%;
  height: 4px;
  background-color: white;
  opacity: 50%;
  transform: skewY(15deg);
  z-index: 5;
`;

export const UpwardTriangle = styled.div`
  margin: 50% 0 -50% -100%;
  width: 200%;
  height: 8px;
  background-color: white;
  opacity: 50%;
  transform: skewY(120deg);
  z-index: 5;
`;