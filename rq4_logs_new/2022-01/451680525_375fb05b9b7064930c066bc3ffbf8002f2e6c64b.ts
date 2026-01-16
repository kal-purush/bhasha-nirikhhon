import styled from "styled-components";

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    align-items:center;
    justify-content:center;
    img{
        width: 100px;
        height: 100px;
        border-radius: 100%;
        object-fit: cover;
        padding: 20px 30px;
    }
    span{
        font-size: 12px;
        text-align:center;
    }
    width: 260px;
    background: #13141E;
    border-radius: 30px;
    margin: 20px 0px 20px 20px;
`;

export const Div = styled.div`
    display: flex;
    margin: 20px 0px;
    box-sizing:border-box;
    justify-content:space-around;
    align-items:center;
    width: 100%;
    button{
        background: #000000;
        color: #fff;
        border-radius: 100px;
        border: 0px;
        padding: 5px 10px;
        :hover{
            background: red;
        }
    }
    span{
        font-size:10px;
    }
`;