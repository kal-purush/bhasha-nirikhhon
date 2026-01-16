import styled from "styled-components";
import Typography, {
  TypographyProps as MuiTypographyProps,
} from "@mui/material/Typography";

interface TypographyProps extends MuiTypographyProps {
  component: React.ReactNode;
  to: string;
}

export const StyledForgotPasswordLink = styled(Typography)<TypographyProps>`
  text-decoration: none;
  color: ${({ theme }) => theme.palette.text.primary};
  &:hover {
    text-decoration: underline;
  }
`;