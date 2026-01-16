import { styled } from "stitches.config";

export const Button = styled('button', {
  width: 'auto',
  textTransform: 'uppercase',
  boxShadow: '0px 4px 4px rgba(0, 0, 0, 0.25)',
  padding: '0 0.7rem',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  borderRadius: '$md',
  background: '$background',
  color: '$textOnBackground',
  fontWeight: 'bold',
  height: '2.5rem',
  transition: '.2s',

  '&:hover': {
    background: '$heading',
  },

  variants: {
    fullWidth: {
      true: {
        width: '100%'
      }
    }
  }
})