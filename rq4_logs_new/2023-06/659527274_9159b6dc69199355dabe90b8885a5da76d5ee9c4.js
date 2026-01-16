import { styled } from "styled-components";

import { Link } from "react-router-dom"
export const Container= styled(Link)`
    display:flex;
    align-items:center;
    margin-bottom:2rem;
    width:100%;
    p{
    color:${({theme})=>theme.COLORS.Light_Light_100};
    font-size:${({fontSize})=>fontSize}px;
    }

`