import styled from 'styled-components'
import theme from '@/styles/theme'
import { StylesConfig, OptionTypeBase } from 'react-select'

export const selectStyles: StylesConfig<OptionTypeBase, false> = {
  control: provided => ({
    ...provided,
    border: 0,
    outline: 0,
    boxShadow: 'none',
    background: 'transparent',
    cursor: 'pointer'
  }),

  indicatorSeparator: provided => ({
    ...provided,
    backgroundColor: 'transparent'
  }),

  dropdownIndicator: (provided, state) => ({
    ...provided,
    color: state.isFocused ? 'lightgreen' : 'lightgrey',
    '&:hover': { color: state.isFocused ? 'lightgreen' : 'lightgrey' }
  }),

  option: (provided, state) => ({
    ...provided,
    cursor: 'pointer',
    backgroundColor: state.isSelected ? 'lightgreen' : 'white'
  })
}

export const BodySelect = styled.div`
  width: 100%;

  .react-select-container {
    cursor: pointer;
    width: 100%;
    text-align: left;
  }
`