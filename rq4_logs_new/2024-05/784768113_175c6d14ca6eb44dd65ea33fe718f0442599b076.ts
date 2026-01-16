import { style } from '@vanilla-extract/css';

export const selectContainer = style({
  position: 'relative',
  paddingBottom: '0.8rem',
});

export const selectButton = style({
  minWidth: '10rem',
  height: '3rem',
  borderRadius: '0.5rem',
  border: '1px solid #c4c4c4',
  background: '#fff',
  textAlign: 'left',
  padding: '0 1em',
  position: 'relative',
  selectors: {
    '&.on, &:focus': {
      outline: 'none',
      borderColor: '#fdd65f',
    },
    '&::after': {
      content: '""',
      position: 'absolute',
      top: '55%',
      right: '1em',
      marginTop: '-0.5rem',
      boxSizing: 'border-box',
      borderTop: '0.5rem solid #c4c4c4',
      borderLeft: '0.4rem solid transparent',
      borderRight: '0.4rem solid transparent',
      transition: 'transform 0.2s',
    },
    '&.on::after': {
      transform: 'rotate(-180deg)',
    },
  },
});

export const optionList = style({
  position: 'absolute',
  top: '3.5rem',
  left: 0,
  zIndex: 10,
  minWidth: '10rem',
  border: '1px solid #c4c4c4',
  borderRadius: '0.5rem',
  background: '#fff',
  display: 'none',
  selectors: {
    [`${selectButton}.on + &`]: {
      display: 'block',
    },
  },
});

export const optionItem = style({
  padding: '0.2rem',
});

export const optionButton = style({
  width: '100%',
  height: '1.5rem',
  display: 'flex',
  alignItems: 'center',
  padding: '1rem',
  textAlign: 'left',
  borderRadius: '0.5rem',
  background: '#fff',
  transition: 'background-color 0.2s',
  selectors: {
    '&:hover, &:focus': {
      backgroundColor: '#fff3cf',
    },
    '&:focus': {
      outline: '1px solid #fdd65f',
    },
    '&.selected': {
      backgroundColor: '#fff3cf',
    },
  },
});