import styled from "styled-components";

export interface IProfilePictureProps {
    BackGroundcolor: string;
    widthAndHeight: string;
    fontSize:string
}

export const StyledBackgroundPicture = styled.div<IProfilePictureProps>`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 100%;
    font-family: "Inter";
    font-style: normal;
    color: #FFFFFF;
    background-color: var(${(props) => props.BackGroundcolor});
    width: ${(props) => props.widthAndHeight};
    height: ${(props) => props.widthAndHeight};
    font-size: ${(props) => props.fontSize};
`;

export default StyledBackgroundPicture;