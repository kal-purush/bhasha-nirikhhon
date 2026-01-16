import styled, { keyframes } from "styled-components";

// Animations
const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateZ(-200px);
  }
  to {
    opacity: 1;
    transform: translateZ(0);
  }
`;

const float = keyframes`
  0% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(-20px);
  }
  100% {
    transform: translateY(0);
  }
`;

const rotate = keyframes`
  0% {
    transform: rotateY(0);
  }
  100% {
    transform: rotateY(360deg);
  }
`;

// Styled Components
export const HeroContainer = styled.section`
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  padding: 4rem 2rem;
  background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
  min-height: 100vh;
  animation: ${fadeIn} 1s ease-out;
  perspective: 1000px;
  overflow: hidden;
`;

export const HeroBg = styled.div`
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
  z-index: -1;
  background: url('https://api.unsplash.com/search/photos?page=1&query=technology') center/cover no-repeat;
  transform: translateZ(-200px) scale(1.5); /* 3D depth effect */
`;

export const HeroInnerContainer = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  max-width: 1200px;
  width: 100%;
  transform: rotateY(-10deg); /* Adding 3D transform */
`;

export const HeroLeftContainer = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 1rem;
  transform: translateZ(50px); /* 3D depth effect */
  animation: ${float} 6s ease-in-out infinite;
`;

export const HeroRightContainer = styled.div`
  flex: 1;
  display: flex;
  justify-content: center;
  align-items: center;
  transform: translateZ(50px); /* 3D depth effect */
`;

export const Img = styled.img`
  max-width: 100%;
  height: auto;
  border-radius: 10px;
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
  transition: transform 0.3s ease-in-out;
  transform: translateZ(100px) rotateY(0); /* 3D depth effect */
  animation: ${rotate} 20s infinite linear;

  &:hover {
    transform: scale(1.05) translateZ(100px);
  }
`;

export const TextLoop = styled.div`
  font-size: 1.5rem;
  color: #fff;
  margin: 1rem 0;
  display: flex;
  align-items: center;
`;

export const Title = styled.h1`
  font-size: 3rem;
  color: #fff;
  margin-bottom: 1rem;
  line-height: 1.2;
  text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.5);
  transform: translateZ(50px); /* 3D depth effect */
`;

export const Span = styled.span`
  font-weight: bold;
  color: #ff6347;
  margin-left: 0.5rem;
`;

export const SubTitle = styled.p`
  font-size: 1.2rem;
  color: #fff;
  margin-bottom: 2rem;
  max-width: 600px;
  line-height: 1.6;
  text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.5);
  transform: translateZ(50px); /* 3D depth effect */
`;

export const HContainer = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: start;
  margin-top: 2rem;
  transform: translateZ(50px); /* 3D depth effect */
`;

export const SocialMediaIcons = styled.div`
  display: flex;
  justify-content: center;
  margin-top: 1rem;
`;

export const SocialMediaIcon = styled.a`
  margin: 0 0.5rem;
  font-size: 1.5rem;
  color: #fff;
  transition: color 0.3s;

  &:hover {
    color: #ff6347; /* Example hover color */
  }
`;

export const CustomButton = styled.a`
  display: inline-block;
  padding: 0.8rem 1.2rem;
  margin: 0.5rem 1rem 0.5rem 0;
  border-radius: 25px;
  text-decoration: none;
  font-weight: 600;
  transition: background-color 0.3s, color 0.3s;
  cursor: pointer;

  &.primary {
    background-color: #007bff;
    color: #fff;
    border: 2px solid #007bff;

    &:hover {
      background-color: #0056b3;
      border-color: #0056b3;
    }
  }

  &.secondary {
    background-color: #6c757d;
    color: #fff;
    border: 2px solid #6c757d;

    &:hover {
      background-color: #5a6268;
      border-color: #5a6268;
    }
  }
`;