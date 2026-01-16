import styled from 'styled-components/native';
import errorOutlineIcon from '../../../assets/error-outline.png';

export const Container = styled.View(({ theme }) => ({
  flex: 1,
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: theme.colors.neutral15,
}));

export const ErrorImage = styled.Image.attrs(() => ({
  source: errorOutlineIcon,
}))(({ theme }) => ({
  marginBottom: theme.spacingSizes.spacing12px,
}));

export const Text = styled.Text(({ theme }) => ({
  fontSize: theme.fontSizes.paragraphMedium,
  fontFamily: theme.fontFamily.interMedium,
  color: theme.colors.neutral100,
  textAlign: 'center',
  paddingBottom: theme.spacingSizes.spacing12px,
}));

export const ReloadButton = styled.TouchableOpacity(({ theme }) => ({
  backgroundColor: theme.colors.feedbackInfo,
  borderRadius: 4,
  padding: theme.spacingSizes.spacing8px,
}));

export const ReloadButtonText = styled.Text(({ theme }) => ({
  fontSize: theme.fontSizes.paragraphSmall,
  fontFamily: theme.fontFamily.interMedium,
  color: theme.colors.neutralWhite,
}));