import { chakra, Flex as CkFlex, Text as CkText } from '@chakra-ui/react'

export const DatePickerWrapper = chakra(CkFlex, {
  baseStyle: () => ({
    border: 'solid 1px',
    borderColor: 'background.grey.200',
    whiteSpace: 'nowrap',
    paddingLeft: { base: 4, lg: 4 },
    paddingRight: { base: 2, lg: 2 },
    paddingY: '7px',
    borderRadius: '6px',
    justifyContent: 'space-between'
  })
})

export const DatePickerText = chakra(CkText, {
  baseStyle: () => ({
    color: 'text.grey.800',
    fontSize: '1rem'
  })
})