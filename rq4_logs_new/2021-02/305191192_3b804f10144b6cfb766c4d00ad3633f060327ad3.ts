import styled from 'styled-components';
import TextField from '@material-ui/core/TextField';
import {Button} from '@material-ui/core';
import IconButton from '@material-ui/core/IconButton';

export const StyledCloseButton = styled(IconButton)`
    position: absolute;
`;


export const StyledButton = styled(Button)`    
    height: 40px;
    margin: 15px;    
`; 

export const AddUrineContainer = styled.div`
    padding: 20px;
    width: 100%;
    margin-left: 60px;   
`;

export const StyledTextField = styled(TextField)`
    margin: 3px 10px;
    width: 250px;
`;

export const StyledHeader = styled.h4`
    margin-left: 120px;
    margin-top: 10px;
    margin-bottom: 0px;
`;

export const StyledDateTime = styled.div`
    padding: 10px;
`;