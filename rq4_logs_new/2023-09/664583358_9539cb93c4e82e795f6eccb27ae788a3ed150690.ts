import { styled } from "@mui/material/styles"
import { Box } from "@mui/material"
import { OutlineButton } from "../../styles/buttons.styles"

export const DetailView = styled(Box)(() => ({
  position: 'relative',
  overflow: 'hidden',
}))

export const CoverBox = styled(Box)(() => ({
  position: 'absolute', 
  bottom: 0, 
  width: '100%', 
  boxSizing: 'border-box',
}))