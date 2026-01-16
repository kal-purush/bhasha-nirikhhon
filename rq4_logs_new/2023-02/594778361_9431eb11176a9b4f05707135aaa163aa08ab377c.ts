import * as Accordion from '@radix-ui/react-accordion'
import { CaretDown } from 'phosphor-react'
import { keyframes, styled } from '../../styles'
import { Text } from '../Text'

export const AccordionRoot = styled(Accordion.Root, {
  borderRadius: 6,
  width: 300,
  backgroundColor: '$gray700',
  border: '1px solid $gray800',
})

export const AccordionItem = styled(Accordion.Item, {
  overflow: 'hidden',
  marginTop: 1,

  transition: 'all 0.2s cubic-bezier(0.87, 0, 0.13, 1)',

  '&:first-child': {
    marginTop: 0,
    borderTopLeftRadius: 4,
    borderTopRightRadius: 4,
  },

  '&:last-child': {
    borderBottomLeftRadius: 4,
    borderBottomRightRadius: 4,
  },

  '&:focus-within': {
    position: 'relative',
    zIndex: 1,
    boxShadow: '0 0 0 2px $colors$cyan300',
  },

  '& + &': {
    borderTop: '1px solid $colors$cyan300',
  },
})

export const StyledHeader = styled(Accordion.Header, {
  all: 'unset',
  display: 'flex',
})

export const StyledTrigger = styled(Accordion.Trigger, {
  all: 'unset',
  fontFamily: 'inherit',
  backgroundColor: 'transparent',
  padding: '0 20px',
  height: 45,
  flex: 1,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  boxShadow: '0 1px 0 $colors$cyan300',

  transition: 'all 0.2s cubic-bezier(0.87, 0, 0.13, 1)',

  '&:hover': { backgroundColor: '$cyan500' },
})

export const StyledChevron = styled(CaretDown, {
  color: '$cyan300',
  transition: 'all 0.2s cubic-bezier(0.87, 0, 0.13, 1)',

  '[data-state=open] &': { transform: 'rotate(180deg)' },
})

export const slideDown = keyframes({
  from: { height: 0 },
  to: { height: 'var(--radix-accordion-content-height)' },
})

export const slideUp = keyframes({
  from: { height: 'var(--radix-accordion-content-height)' },
  to: { height: 0 },
})

export const StyledContent = styled(Accordion.Content, {
  overflow: 'hidden',
  backgroundColor: '$gray600',

  '&[data-state="open"]': {
    animation: `${slideDown} 300ms cubic-bezier(0.87, 0, 0.13, 1)`,
  },
  '&[data-state="closed"]': {
    animation: `${slideUp} 300ms cubic-bezier(0.87, 0, 0.13, 1)`,
  },
})

export const StyledContentText = styled(Text, {
  padding: '15px 20px',
})