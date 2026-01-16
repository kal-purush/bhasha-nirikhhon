import { styled } from "@mui/material/styles"
import { Box } from "@mui/material"

export const ProductInfomation = styled(Box)(() => ({
  display: 'flex', 
  alignItems: 'center', 
  padding: '16px', 
  background: '#FAFAFA',
  '& .image': {
    flex: '0 0 56px', 
    display: 'flex', 
    alignItems: 'center', 
    marginRight: '8px', 
    height: '56px', 
    overflow: 'hidden', 
    '& > img': { 
      display: 'block', 
      width: '100%',
    }
  },
  '& > .text > p': { 
    color: '#333', 
    fontWeight: 500,
  }, 
  '& > .text .brand': { 
    fontSize: '.75rem',
  }, 
  '& > .text .product': { 
    fontSize: '.875rem',
  },
}))

export const VisuallyHiddenInput = styled('input')({
  clip: 'rect(0 0 0 0)',
  clipPath: 'inset(50%)',
  height: 1,
  overflow: 'hidden',
  position: 'absolute',
  bottom: 0,
  left: 0,
  whiteSpace: 'nowrap',
  width: 1,
})

export const QnaTextField = styled(Box)(() => ({
  '& > input': {
    marginTop: '16px',
    width: '100%',
    border: '1px solid #BDBDBD',
    background: '#FAFAFA',
    fontSize: '.75rem',
    padding: '14px',
    boxSizing: 'border-box',
  },
  '& > input + textarea': {
    marginTop: '16px',
    width: '100%',
    border: '1px solid #BDBDBD',
    fontSize: '.75rem',
    padding: '14px',
    boxSizing: 'border-box',
  }
}))

export const QnaImageField = styled(Box)(() => ({
  display: 'flex',

  '& > .image_box_wrap': {
    position: 'relative',
    width: '25%',
    paddingTop: '25%',
    '& > div': {
      position: 'absolute',
      top: 0,
      left: 0,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      width: '100%',
      height: '100%',
      overflow: 'hidden',
      background: '#000',
    },
    '& > div > img': {
      maxWidth: '100%',
      maxHeight: '100%',
    },
  },
  '& > .MuiButtonBase-root': {
    position: 'relative',      
    width: '25%',
    padding: '25% 0 0 0',
    borderRadius: 0,
    backgroundColor: '#242223',
    color: '#fff',
    '&:hover': {
      backgroundColor: '#242223',
    },
    '& > svg': {
      position: 'absolute',
      top: 'calc(50% - 8px)',
      left: 'calc(50% - 8px)',
    }
  }
}))

export const UploadNotice = styled(Box)(() => ({ 
  fontSize: '.75rem', 
  color: '#BDBDBD', 
  'p > span': { 
    color: '#FF5454' 
  } 
}))