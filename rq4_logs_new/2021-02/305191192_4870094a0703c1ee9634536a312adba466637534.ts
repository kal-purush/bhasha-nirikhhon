import styled from 'styled-components';
import {NavLink} from 'react-router-dom';
import {Button} from '@material-ui/core';


export const StyledContainer = styled.div`
    height: 100%;
    width: 95%;
    margin: 5px;

    .MuiDataGrid-colCell {
        font-size: 18px;
        background-color: gray;
        color: white;
    }
`;

export const StyledNavLink = styled(NavLink)`
    text-decoration: none;
`;

export const StyledButton = styled(Button)`
    width: 200px;
    height: 40px;
    margin: 15px;
`;