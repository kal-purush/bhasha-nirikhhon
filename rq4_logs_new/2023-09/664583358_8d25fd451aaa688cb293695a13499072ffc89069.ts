import styled from "@emotion/styled"
import { Box } from "@mui/material"
import { GridBox } from "../Box"

export const BrandContainer = styled(GridBox)(() => ({
  gridTemplateColumns: 'fit-content(100%) auto',
  borderTop: '1px solid #DADADA',
  '&:last-child': {
    borderBottom: '1px solid #DADADA'
  }
}))

export const BrandImageBox = styled(Box)(() => ({
  position: 'relative',
  padding: '1rem 1rem 0 0',
  marginBottom: '1rem',
  borderRight: '1px solid #DADADA',
  '& img': {
    aspectRatio: '10 / 10',
    width: '5rem',
    height: '100%',
  },
  '& .rank': {
    position: 'absolute',
    bottom: 0,
    width: '1.5rem',
    height: '1.5rem',
    backgroundColor: '#000',
    color: '#fff',
    fontSize: '14px',
    lineHeight: '16.8px'
  }
}))

export const BrandDetailBox = styled(Box)(() => ({
  width: '100%',
  padding: '16px 8px 16px 18px',
  '& .css-8tddpf-MuiTypography-root': {
    marginBottom: '0.31rem'
  }
}))