import React from 'react';
import styled from 'styled-components';
import agendaIcon from '../agenda-icon.png';
import todoListIcon from '../tasks-icon.png';
import logo from '../logo.png'; // Dodane logo
import BottomBar from '../components/BottomBar';
import NavigationBar from "../components/NavigationBar";
import Button from "../components/Button";

const HomeContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 90.5vh;
`;

const Title = styled.h1`
    font-size: 2rem;
    margin-bottom: 70px; // Zwiększenie odstępu od tytułu
`;

const ContentContainer = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 40px;
`;

const LogoContainer = styled.div`
    margin-right: 100px; // Zmniejszenie odstępu między logiem a treścią
`;

const LogoImage = styled.img`
    width: 350px; // Zwiększenie szerokości loga
    height: auto;
`;

const ImageContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-bottom: 40px;
`;

const Image = styled.img`
    width: 200px;
    height: auto;
    margin: 0 20px;
    cursor: pointer;
`;

const TabContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const HomePage = () => {
    return (
        <>
            <NavigationBar />
            <HomeContainer>
                <Title>TimeWise Pro</Title>
                <ContentContainer>
                    <LogoContainer>
                        <LogoImage src={logo} alt="Logo" />
                    </LogoContainer>
                    <TabContainer>
                        <Button>Agenda</Button>
                        <Button>To Do List</Button>
                    </TabContainer>
                </ContentContainer>
            </HomeContainer>
            <BottomBar />
        </>
    );
};

export default HomePage;