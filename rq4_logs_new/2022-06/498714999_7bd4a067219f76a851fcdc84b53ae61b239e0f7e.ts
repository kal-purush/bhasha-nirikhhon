import { styled } from "stitches.config";

export const Container = styled('div', {
  width: '100%',
  borderRadius: '$md',
  background: '$foreground',
  padding: '27px',
  display: 'flex',
  gap: '$sm'
})

export const Thumb = styled('div', {
  width: '6.25rem',
  height: '6.25rem',
  borderRadius: '$sm',
  overflow: 'hidden',
  position: 'relative'
})

export const Name = styled('strong', {
  fontSize: '$md',
})

export const Id = styled('span', {
  fontSize: '$sm',
})


export const Amount = styled('span', {
  fontSize: '$md',
  fontWeight: '500'
})