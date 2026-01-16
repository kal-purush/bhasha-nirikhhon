import type { SxProps } from '@mui/material';

interface AddProfileMultiplechipDropdownStyleProps {
  [key: string]: SxProps;
}

export const addProfileMultiplechipDropdownStyle: AddProfileMultiplechipDropdownStyleProps = {
  rootSx: {},
    dialogSx: {
    width: '400px',
    height: '358px',
  },
  padd: {
    p: '24px',
  },
  labelSx: {
    fontSize: '12px',
    color: '#262C33',
    fontWeight: '500',
    pb: 1,
  },

  inputGroupSx: {
    display: 'grid',
    // pb: 2,
    // mt: '9px',
  },
  inputSx: {
    '& .MuiOutlinedInput-input': {
      fontWeight: 500,
      fontSize: '14px',
    },
  },
};
