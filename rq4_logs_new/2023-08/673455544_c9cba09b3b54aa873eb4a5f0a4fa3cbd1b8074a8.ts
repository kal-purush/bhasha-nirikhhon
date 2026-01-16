import { styled } from "styled-components";

export const DivHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.7rem;

  background: var(--white);

  @media (min-width: 768px) {
    position: fixed;
    top: 0;
    right: 0;
    z-index: 9;

    width: 100%;
    padding: 1rem 3.75rem;

    box-shadow: rgba(0, 0, 0, 0.1) 0px 4px 12px;
  }
`;

export const Nav = styled.nav`
  display: flex;
  gap: 1rem;
`;

export const ButtonHeader = styled.button`
  background: none;
  border-radius: 6px;

  cursor: pointer;

  padding: 10px 20px;

  font-size: 0.7rem;
  font-weight: 600;
  color: var(--primary-color);

  transition: 0.2s ease;

  @media (min-width: 768px) {
    border: 1px solid var(--primary-color);
    font-size: 0.9rem;

    &:hover {
      border: 1px solid var(--white);
      background: var(--black);
      color: var(--white);
    }
  }
`;

export const MobileNav = styled.div`
  display: flex;
  flex-direction: column;
  gap: 10px;
  align-items: flex-end;
`;

export const UserHeaderContainer = styled.div`
  display: flex;
  align-items: center;
  gap: .4rem;
  
  cursor: pointer;

  > span:nth-child(1) {
    background: var(--primary-color);

    color: var(--white);

    border-radius: 50%;

    padding: .4rem .7rem;
  }

  > span:nth-child(2) {
    color: var(--gray);

    font-size: .8rem;
  }
`