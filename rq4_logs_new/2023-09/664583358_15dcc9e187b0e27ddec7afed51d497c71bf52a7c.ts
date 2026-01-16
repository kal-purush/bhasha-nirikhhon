import { css } from "@emotion/css"
import { styled } from "@mui/material/styles"
import { Modal } from "@mui/material"

export const BasicModal = styled(Modal)(() => ({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    '& > .MuiBox-root': {
      width: 400,
      margin: '0 16px',
      '& .content': {
        padding: '40px 32px',
        backgroundColor: 'white',
        textAlign: 'center',
      }
    },
    '& .MuiButtonGroup-root > button': {
      borderRadius: 0, 
      border: 'none', 
      height: '64px'
    }
  }))