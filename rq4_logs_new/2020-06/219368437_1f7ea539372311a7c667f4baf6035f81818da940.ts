import styled from 'styled-components';
import { lighten } from 'polished';

export const StyledButton = styled.button<{ alternative: boolean }>`
  background-color: ${({ theme, alternative }): string => alternative ? theme.title : theme.main};
  color: white;
  font-weight: 600;
  padding: 5px 20px;
  border-radius: 3px;
  box-shadow: 6px 3px 6px -3px rgba(0,0,0,0.16);
  transition: background-color .2s ease-in-out, transform .2s ease-in-out;

  &:hover{
    background-color: ${({ theme, alternative }): string => lighten(0.05, alternative ? theme.title : theme.main)}!important;
    transform: translateY(-3px);
  }
`;