import React from 'react';
import { useSelector } from 'react-redux';
import { getAuthIsLoggedIn } from '../redux/auth/authSelectors';

import UserMenu from 'components/UserMenu/UserMenu';
import AuthNav from 'components/authNav';

import { HomeStyle, StyledLink } from 'components/App.styled';

export default function AppBar() {
  const isLoggedIn = useSelector(getAuthIsLoggedIn);
  return (
    <HomeStyle>
      <header>
        <div>
          <StyledLink to="/home">Home</StyledLink>
          <StyledLink to="/contacts">Contacts</StyledLink>
        </div>

        {isLoggedIn ? <UserMenu /> : <AuthNav />}
      </header>
    </HomeStyle>
  );
}