import { Select } from '@mui/material';
import { styled } from '@mui/material/styles';

export const SelectCustom = styled(Select)({
    // LABEL
    '& label': {
        color: '#48539b',
        paddingLeft: 6,

        '&.Mui-focused': {
            color: '#8179B5',
        }
    },

    '& label.Mui-disabled':{
        color: 'white !important',
    },

    '& .css-1sqnrkk-MuiInputBase-input-MuiOutlinedInput-input.Mui-disabled':{
        "-webkit-text-fill-color": 'white !important',
    },

    ' .MuiOutlinedInput-input':{
        "-webkit-text-fill-color": 'white !important',
    },

    //  INPUT
    '& input': {
        color: 'white',

        '&:valid:focus + fieldset': {
            borderLeftWidth: 8
        },
        '&.Mui-focused': {
            color: '#8179B5',
        },
        '&.Mui-disabled': {
            "-webkit-text-fill-color": 'white !important',
            borderColor: '#48539b', 
        },
        
    },

    '& .MuiOutlinedInput-root': {
        '& fieldset': {
            borderColor: '#48539b',
        },
        '&:hover fieldset': {
            borderColor: '#3E3C7C',
            color: '#3E3C7C'
        },
        '&.Mui-focused fieldset': {
            borderColor: '#8179B5',
        },
    },
});