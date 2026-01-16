import styled from "styled-components";

export const Container = styled.div`
    display: flex;
    align-items:center;
    justify-content:space-between;
    flex-direction: column;
    padding: 10px 10px;
    min-width:150px;
    max-width:150px;
    img{
        min-width:100px;
        width: 100px;
        height: 100px;
        border-radius: 100%;
        object-fit: cover;
        margin-right:10px;
    }
    span{
        font-size: 12px;
        text-align:center;
    }
    background: #13141E;
    border-radius: 30px;
    margin: 20px 0px 20px 10px;
    @media(min-width: 830px){
        flex-direction: row;
        max-width:350px;
        justify-content:flex-start;
    }
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

export const ContainerAmount = styled.div`
    display: flex;
    align-items: center;
    margin-top: 20px;
    button{
        margin-right: 10px;
        background: #FD0303;
        border: 0px;
        color: white;
    }
    .btn-add{
        margin-right: 0px;
        margin-left:10px;
        background: #468C00;
    }

`;