import { scales, variants } from './types';

export const scaleVariants = {
  [scales.MD]: {
    height: '3.75rem',
    padding: '0 40px',
  },
  [scales.SM]: {
    height: '3.1rem',
    padding: '0 30px',
  },
  [scales.XS]: {
    height: '2.5rem',
    padding: '0 20px',
  },
};

export const styleVariants = {
  [variants.PRIMARY]: {
    backgroundColor: 'primary',
    color: 'white',
  },
  [variants.PRIMARY_BASIC]: {
    backgroundColor: 'white',
    border: '2px solid',
    borderColor: 'primary',
    color: 'primary',
    ':disabled': {
      backgroundColor: 'transparent',
      color: 'gray',
    },
  },
  [variants.SECONDARY]: {
    backgroundColor: 'secondary',
    color: 'white',
    ':disabled': {
      backgroundColor: '#E9EAEB',
      borderColor: '#E9EAEB',
    },
  },
  [variants.SECONDARY_BASIC]: {
    backgroundColor: 'white',
    border: '2px solid',
    borderColor: 'secondary',
    color: 'secondary',
    ':disabled': {
      backgroundColor: 'transparent',
      color: '#E9EAEB',
      borderColor: '#E9EAEB',
    },
  },
  [variants.TEXT]: {
    backgroundColor: 'transparent',
    color: 'primary',
  },
};