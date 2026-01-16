import { styled } from "@mui/material/styles"
import { Box, FormControl } from "@mui/material"

interface SelectProps {
  isopen: string
}

export const OutlinedTag = styled(Box)(() => ({
  display: 'flex',
  alignItems: 'center',
  flex: '1 1 auto',
  boxSizing: 'border-box',
  padding: '12.7px 8px 12.7px 16px',
  maxWidth: '48%',
  borderRadius: 0,
  border: '1px solid #DADADA',
  color: '#999999',
  '& > img': {
    width: '18px',
    marginRight: '4px',
  }
}))

export const OutlinedCheckbox = styled(FormControl)(() => ({
  '& input': {
    display: 'none',
  },
  '& label': {
    display: 'flex',
    alignItems: 'center',
    padding: '6.5px 12px',
    borderRadius: '18px',
    border: '1px solid #DADADA',
    fontSize: '0.875rem',
  },
  '& input:checked + label': {
    border: '1px solid #242223',
    backgroundColor: '#242223',
    color: '#fff',
  },
  '& label > img': {
    width: '18px',
    marginRight: '4px',
  },
}))

export const OutlinedSelect = styled(Box)<SelectProps>((props) => ({
  position: 'relative',
  '&::before': {
    content: '"⌵"',
    position: 'absolute',
    top: '8px',
    right: '16px',
    color: '#DADADA',
    fontSize: '20px',
  },
  '& > .MuiButton-root': {
    padding: '6px 16px',
    minHeight: 52,
    borderRadius: 0,
    border: '1px solid #DADADA',
    justifyContent: 'start',
    color: '#999'
  },
  '& > .MuiButton-root > img': {
    width: '18px',
    marginRight: '4px',
  },
  '& .dropmenu': {
    display: props.isopen === 'true' ? 'block' : 'none',
    marginTop: '-1px',
    padding: 0,
    color:'#999999',
    border: '1px solid #dadada',
  },
  '& .dropmenu > li': {
    padding: 0,
  },
  '& .dropmenu > li:not(:last-of-type)': {
    borderBottom: '1px solid #dadada',
  },
  '& .dropmenu .MuiListItemButton-root': {
    padding: '13.5px 16px',
  },
  '& .dropmenu .MuiListItemButton-root > img': {
    width: '18px',
    marginRight: '4px',
  }
}))