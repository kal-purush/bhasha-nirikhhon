import styled from 'styled-components';
import { Hashtag } from 'styled-icons/heroicons-outline';
import { PersonAdd, Settings } from 'styled-icons/material';

interface Props {
  selected?: boolean;
}

export const Container = styled.div<Props>`
  display: flex;
  align-items: center;
  justify-content: space-between;

  cursor: pointer;

  padding: 5px 3px;
  border-radius: 5px;

  background-color: transparent;
  transition: background-color 0.2s;

  > div {
    display: flex;
    align-items: center;
  }

  > div span {
    font-size: 15px;
    margin-left: 5px;
    color: var(--senary);
  }

  > div:nth-child(2) {
    display: none;
  }

  &:hover,
  &:active {
    background-color: var(--quinary);
    > div:last-child {
      display: block;
    }

    > div {
      span {
        color: var(--white);
      }
    }
  }
`;

export const HastagIcon = styled(Hashtag)`
  width: 20px;
  height: 20px;

  color: var(--symbol);
`;

export const InviteIcon = styled(PersonAdd)`
  width: 16px;
  height: 16px;
  margin-right: 5px;

  color: var(--symbol);
  cursor: pointer;

  transition: color 0.2s;

  &:hover {
    color: var(--white);
  }
`;

export const SettingsIcon = styled(Settings)`
  width: 16px;
  height: 16px;

  color: var(--symbol);
  cursor: pointer;

  transition: color 0.2s;

  &:hover {
    color: var(--white);
  }
`;