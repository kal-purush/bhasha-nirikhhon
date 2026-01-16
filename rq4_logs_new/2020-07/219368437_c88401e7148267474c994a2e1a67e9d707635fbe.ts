import styled from 'styled-components';
import { SCREEN } from 'lib/enums';

export const Container = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: -1;

  @media ${SCREEN.lg}{
    flex-direction: column;
  }
  @media ${SCREEN.xs}{
    margin-bottom: 50px;
  }
`;

export const ArrowImg = styled.img`
  width: 150px;
  height: auto;

  @media ${SCREEN.lg}{
    display: none;
  }
`;

export const ArrowDownImg = styled.img`
  width: 150px;
  height: auto;
  display: none;

  @media ${SCREEN.lg}{
    display: block;
  }
`;

export const TextContainer = styled.div`
  font-family: 'Nanum Pen Script', cursive;
  color: ${({ theme }): string => theme.main};
  font-size: 2rem;
  margin: 60px 0 0 15px;

  @media ${SCREEN.lg}{
    margin: 0;
    text-align: center;
  }
`;