import styled from 'styled-components';
import Paper from '@material-ui/core/Paper';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import Button from '@material-ui/core/Button';
import TextFieldBase from '@material-ui/core/TextField';

export const Container = styled.div`
  display: flex;
  min-height: 100vh;
  background: #262a41;
  justify-content: center;
  align-items: center;
  padding: 30px;
`;

export const Card = styled(Paper)`
  display: flex;
  flex: 1;
  height: calc(100vh - 60px);
  overflow: hidden;
  max-width: 1000;
`;

export const Title = styled(Typography).attrs({
  variant: 'h5',
  component: 'h3'
})`
  margin-bottom: 10px !important;
  color: #262a41;
`;

export const ConfigPanel = styled.div`
  display: flex;
  flex-direction: column;
  width: 350px;
  min-width: 350px;
  padding: 10px;
  background: #cecece;
`;

export const PreviewPanel = styled.div`
  display: flex;
  flex: 1;
  padding: 10px;
  flex-direction: column;
`;

export const DragContainer = styled.div`
  display: flex;
  border: 2px #262a41 dashed;
  cursor: pointer;
  padding: 20px;
  align-items: center;
  justify-content: center;
  margin-bottom: 20px;
`;

export const UploadText = styled(Typography).attrs({
  component: 'h6'
})`
  flex: 1;
  margin: 5;
  cursor: pointer;
  text-align: center;
  font-size: 18px;
  color: #262a41;
`;

export const PropsDivider = styled(Divider).attrs({
  light: true
})``;

export const PreviewContainer = styled.div`
  justify-content: center;
  flex: 1;
  display: flex;
  align-items: center;
`;

export const ImagePreview = styled.img`
  width: 400px;
  height: 400px;
  align-self: center;
  justify-self: center;
`;

export const ButtonContainer = styled.div`
  display: flex;
  flex-direction: row;
  margin-top: 20px;
  margin-bottom: 10px;
`;

export const ClearButton = styled(Button).attrs({
  variant: 'contained'
})`
  margin-right: 10px !important;
  flex: 1;
`;

export const GenerateButton = styled(Button).attrs({
  variant: 'contained',
  color: 'primary'
})`
  margin-left: 10px !important;
  flex: 1;
`;

export const OptionsContainer = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
`;

export const ColorPickerPopover = styled.div`
  position: absolute;
  z-index: 2;
`;

export const ColorPickerCover = styled.div`
  position: fixed;
  top: 0px;
  right: 0px;
  bottom: 0px;
  left: 0px;
`;

export const TextField = styled(TextFieldBase).attrs({
  InputLabelProps: {
    style: {
      color: '#262a41'
    }
  },
  fullWidth: true,
  margin: 'normal'
})`
  position: fixed;
  top: 0px;
  right: 0px;
  bottom: 0px;
  left: 0px;
`;