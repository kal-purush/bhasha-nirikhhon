import { style } from '@vanilla-extract/css';

export const container = style({
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
});

export const loginBox = style({
  width: '640px',
  height: '1020px',
  margin: '48px 640px 54px 640px',
});
export const logo = style({
  width: '340px',
  height: '192px',
});
export const logoContainer = style({
  display: 'flex',
  justifyContent: 'center',
  width: '100%',
  marginBottom: '56px',
});
export const form = style({
  display: 'flex',
  flexDirection: 'column',
});

export const formGroup = style({
  marginBottom: '28px',
});
export const label = style({
  fontSize: '16px',
  fontWeight: '400',
  marginBottom: '8px',
  display: 'block',
});
export const loginButton = style({
  width: '640px',
  height: '48px',
  borderRadius: '6px',
  marginBottom: '32px',
});
export const footer = style({
  textAlign: 'center',
  fontSize: '16px',
  marginBottom: '48px',
});
export const linkText = style({
  color: ' #0B3B2D',
  textDecoration: 'underline',
});
export const socialLoginContainer = style({
  display: 'flex',
  textAlign: 'center',
  justifyContent: 'center',
  alignItems: 'center',
  fontSize: '20px',
  color: ' #79747E',
  gap: '37.5px',
  marginBottom: '40px',
});
export const line = style({
  flex: 1,
  height: '1px',
  backgroundColor: '#ccc',
});
export const socialButtons = style({
  display: 'flex',
  justifyContent: 'center',
  gap: '16px',
});