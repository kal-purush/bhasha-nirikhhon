import styled from 'styled-components';

export const CategoryContainer = styled.div`
    position:absolute;
    width:360px;
    height:91px;
    left:0px;
    top:101px;
`;

export const CategoryUl = styled.ul`
    position:absolute;
    width:342px;
    height:90px;
    left:16px;
    top:0;
    margin:0;
    padding:0;

    white-space:nowrap;
    overflow-x:auto;
    overflow-y:hidden;
    &::-webkit-scrollbar{
        display:none;
    }
`;

export const CategoryLi = styled.li`
    position:relative;
    list-style:none;
    display:inline-block;
    margin:0 8px 0 8px;
`;

export const CategoryBar = styled.div`
    position:absolute;
    width:360px;
    height:1px;
    left:0px;
    top:90px;

    &>div{
        position:absolute;
        left:0%;
        right:0%;
        top:100%;
        bottom:-100%;
        background:#DFDFDF;
        opacity:0.9;
        transform:matrix(1,0,0,-1,0,0);
    }
`;