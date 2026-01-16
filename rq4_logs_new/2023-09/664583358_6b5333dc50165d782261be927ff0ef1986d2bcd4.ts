import { styled } from "@mui/material/styles"
import { Box } from "@mui/material"
import { OutlineButton } from "../../../styles/buttons.styles"

export const DetailView = styled(Box)(() => ({
  position: 'relative',
  overflow: 'hidden',
  
  '& img': { 
    display: 'block',
    width: '100%',
  }
}))

export const MoreButton = styled(OutlineButton)(() => ({
  padding: '15px 8px',
  borderRadius: 0,
  fontSize: '1rem',
}))

export const CoverBox = styled(Box)(() => ({
  padding: '40px 16px 16px',
  position: 'absolute', 
  bottom: 0, 
  width: '100%', 
  background: 'linear-gradient(#ffffff00 0%, #fff6 15%, #ffffffbf 30%, #ffffff 50%)',
  boxSizing: 'border-box',
}))

export const MoreInfomation = styled(Box)(() => ({
  padding: '16px 16px 40px',
  
  '& > button': {
    justifyContent: 'space-between',
    padding: '8px',
    borderRadius: 0,
    borderBottom: '1px solid #DADADA',
    color: '#333',
    fontSize: '.875rem',
  }
}))