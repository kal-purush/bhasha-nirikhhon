import type { SxProps } from '@mui/material';

interface SuperAdminTabsStyleProps {
  [key: string]: SxProps;
}

export const superAdminTabsStyle: SuperAdminTabsStyleProps = {
    rootSx: {
    // height: '52px',
    marginTop: '-23px',
    // marginLeft: '-7px',
    backgroundColor: '#FFFFFF',
    border: '1px solid #EAEAEA',
    opacity: 1,
    display: 'flex',
  },
  title: {
    textTransform: 'capitalize',
    fontSize: '16px',
    fontWeight: 600,
    padding: '16px',
    // marginLeft:'12px'
  },
  reportTabs: {
    borderRadius: '6px',
    boxShadow: '0px 4px 10px #0000000A',
    ml: '24px',
  },
  tabBar: {
    '& .css-1gsv261': {
      borderBottom: '0px !important',
    },

    '& .MuiTab-textColorPrimary': {
      fontSize: '13px',
      fontWeight: 600,
      paddingBottom: '6px !important',
      alignItems: 'start !important',
      textTransform: 'capitalize',
      justifyContent: 'end',
    },

    '& .Mui-selected': {
      color: '#357968 !important',
      fontWeight: 600,
    },

    '& .MuiTabs-indicator': {
      display: 'none',
      // backgroundColor: "#357968 !important",
      // height: "3px !important"
    },
    '& .MuiTab-root': {
      minWidth: '10px',
      // padding: '14px 8px 12px 4px ',
    },
  },

  alertConfigTab: {
    cursor: 'pointer',
    fontSize: '14px',
    fontWeight: 500,
    paddingBottom: '18px !important',
    color: '#5A5A5A',
    '&:nth-child(even)': {
      margin: '0 24px ',
    },
  },

  alertConfigTabTxt: {
    cursor: 'pointer',
    fontSize: '14px',
    fontWeight: 500,
    color: '#357968',
    // color: theme.palette.primary.mailInput,
    borderBottom: '3px solid #357968',
    borderRadius: '2px',

    paddingBottom: '0px !important',
    '&:nth-child(even)': {
      margin: '0 24px',
    },
  },
};
