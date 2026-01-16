import { keyframes } from "styled-components";

export const appearFromLeft = keyframes`
from {
    transform: translateX(-50px);
    opacity: 0;
}
to {
    transform: translateX(0px);
    opacity: 1;
}
`;

export const appearFromRight = keyframes`
from {
    transform: translateX(50px);
    opacity: 0;
}
to {
    transform: translateX(0px);
    opacity: 1;
}
`;

export const fadeIn = keyframes`
from {
    opacity: 0;
}
to {
    opacity: 1;
}

`;

export const appearFromTop = keyframes`
from {
    transform: translateY(-50px);
    opacity: 0;
}
to {
    transform: translateY(0px);
    opacity: 1;
}

`;