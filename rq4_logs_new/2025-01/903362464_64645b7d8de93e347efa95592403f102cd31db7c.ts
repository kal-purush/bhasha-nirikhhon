import { style } from '@vanilla-extract/css';

export const pageContainer = style({
  minHeight: '700px',
});

export const container = style({
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  marginBottom: '24px',
});

export const title = style({
  fontSize: '32px',
  fontWeight: '700',
  color: '#000000',
});

export const saveButton = style({
  borderRadius: '4px',
  width: '120px',
  height: '48px',
  alignSelf: 'flex-end',
});

export const form = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '32px',
  width: '100%',
  maxWidth: '792px',
  margin: '0 auto',
});

export const formGroup = style({
  display: 'flex',
  flexDirection: 'column',
  width: '100%',
});

export const label = style({
  fontSize: '24px',
  fontWeight: '700',
  color: '#000000',
  marginBottom: '16px',
});