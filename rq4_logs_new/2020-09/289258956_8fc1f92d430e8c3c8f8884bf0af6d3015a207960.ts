import styled from 'styled-components';
import { ContainerMode } from './@types';

export const ContainerWrapper = styled.div<{
  mode?: ContainerMode;
  padding?: string;
}>`
  display: flex;
  flex-direction: column;
  background: ${(p) => p.theme.containers?.[p.mode || 'l1Container']?.background || 'white'};
  border-radius: ${(p) => p.theme.containers?.[p.mode || 'l1Container']?.borderRadius || '30px'};
  padding: ${(p) => p.padding || '24px'};
  min-width: 50px;
  min-height: 50px;
  color: ${(p) => p.theme.containers?.[p.mode || 'l1Container']?.textColor || p.theme.textColors?.neutral || 'black'};
`;