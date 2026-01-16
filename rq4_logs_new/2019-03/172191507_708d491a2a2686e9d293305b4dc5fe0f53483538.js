import React from 'react';
import Avatar from '@material-ui/core/Avatar';
import styled from 'styled-components';
import Typography from '@material-ui/core/Typography';

const Root = styled.div(({ theme }) => ({
  padding: theme.spacing(2, 1, 2, 1),
  display: 'flex',
  alignItems: 'center',
}));

const Name = styled(Typography)(({ theme }) => ({
  marginLeft: theme.spacing(1),
  fontSize: 24,
}));

export default ({ src, name }) => (
  <Root>
    <Avatar src={src} />
    <Name>{name}</Name>
  </Root>
);