import styled from 'styled-components';

export const Nav = styled.div`
    padding: 0;
    margin: 0;
    box-sizing: border-box;

    .ul {
        list-style: none;
    }

    .a {
        text-decoration: none;
        color: black;
        font-size: 1rem;
    }

    .navbar {
        background-color: beige;
        padding: 10px 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 40px;
        min-height: 70px;
    }

    .logo {
        font-size: 2rem;
        font-family: Arial, Helvetica, sans-serif;
    }

    .navMenu {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 60px;
    }
`;