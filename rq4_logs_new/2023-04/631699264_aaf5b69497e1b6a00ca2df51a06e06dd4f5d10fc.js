import React from 'react'
import styled from 'styled-components'

import SearchIcon from '@mui/icons-material/Search';
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart';

import Badge from '@mui/material/Badge';

import logo from '../media/toy-shop-logo-no-background.png'

const Container = styled.div`
    position: fixed;
    top: 0;
    width: 100vw;
    display: flex;
    justify-content: center;
    align-items: flex-start;
    height: 110px;
    background: white;
`
const Wrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    max-width: 1400px;
    width: 100%;
`

const Left = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
`
const Center = styled.div`
    flex: 1;
    display: flex;
    justify-content: center;
`
const Right = styled.div`
    flex: 1;
    display: flex;
    justify-content: flex-end;
    align-items: center;
`
const SearchContainer = styled.div`
    border: 0.5px solid lightgray;
    display: flex;
    align-items: center;
    padding: 5px;
`
const Input = styled.input`
    border: none;
    &:focus {
        outline: none;
    }
    font-size: 24px;
`
const Logo = styled.div`
    font-weight: bold;
    width: 140px;
    height: 140px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    &>img {
        max-width: 90%;
        height: auto;
    }
    background: white;
`

const MenuItem = styled.div`
    font-size: 24px;
    cursor: pointer;
    margin-left: 30px;
`

const Navbar = () => {
  return (
    <Container>
        <Wrapper>
            <Left>
                <SearchContainer>
                    <Input />
                    <SearchIcon style={{color: "gray", fontSize: "24px"}}/>
                </SearchContainer>
            </Left>
            <Center><Logo><img src={logo} /></Logo></Center>
            <Right>
                <Badge badgeContent={4} color="primary">
                    <ShoppingCartIcon color="action" />
                </Badge>
                <MenuItem>ZAREJESTRUJ</MenuItem>
                <MenuItem>ZALOGUJ</MenuItem>
            </Right>
        </Wrapper>
    </Container>
  )
}

export default Navbar