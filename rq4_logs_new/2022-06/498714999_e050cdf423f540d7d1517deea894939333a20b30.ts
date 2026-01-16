import { styled } from "stitches.config";

export const Container = styled('div', {
  height: '27.18rem',
  width: '100%',
  maxWidth: '20rem',
  background: '$foreground',
  borderRadius: '$md'
})

export const Thumb = styled('div', {
  width: '100%',
  height: '70%',
  BACKGROUND: 'tomato'
})

export const ThumbImg = styled('img', {
  width: '100%',
  height: '100%',
  objectFit: 'cover',
  borderRadius: '$md',
})

export const Body = styled('div', {
  padding: '$sm',
})

export const Name = styled('strong', {
  fontSize: '$md',
  color: '$heading'
})

export const Amount = styled('span', {
  color: '$heading',
  fontSize: '$md',
})

export const Id = styled(Amount, {
  fontSize: '$sm',
})