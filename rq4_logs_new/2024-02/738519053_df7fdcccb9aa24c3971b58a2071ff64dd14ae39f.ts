import styled from 'styled-components';
import theme from 'styles/theme';

export const StyledInputBox = styled.div<{ width: string }>`
	display: flex;
	flex-direction: column;
	gap: 8px;
	${({ width }) => (width ? '' : 'flex-grow: 1;')};
	.error {
		color: ${theme.PALETTE.error};
		font-size: ${theme.FONT_SIZE.small};
	}
`;

export const StyledInput = styled.input<{ error: string }>`
	padding: 0 16px;
	height: 45px;
	line-height: 45px;
	font-size: ${theme.FONT_SIZE.large};
	color: ${theme.PALETTE.fblack};
	border: 1px solid
		${({ error }) => (error ? theme.PALETTE.error : theme.PALETTE.gray[500])};
	box-sizing: border-box;
	&::placeholder {
		color: #ccc;
	}
`;