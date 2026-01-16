import { style } from '@vanilla-extract/css';

export const profileImageContainer = style({
  position: 'relative',
  width: '13rem',
  height: '13rem',
  margin: '2.4rem auto',
});

export const profileUserNameLayout = style({
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  flexDirection: 'column',
});

export const profileImageBox = style({
  backgroundColor: '#E3E5E8',
  width: '100%',
  height: '100%',
  position: 'absolute',
  borderRadius: '50%',
  overflow: 'hidden',
  boxShadow: '0px 4px 16px rgba(0, 0, 0, 0.08)',
});

export const profileImage = style({
  objectFit: 'contain',
});

export const defaultImage = style({
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
});

export const drawerListLayout = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '2.4rem',
});
export const profileMenuListContainer = style({});