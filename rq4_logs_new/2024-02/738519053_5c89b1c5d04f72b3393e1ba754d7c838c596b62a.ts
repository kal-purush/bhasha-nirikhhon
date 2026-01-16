import styled, { css } from 'styled-components';
import { TootipProps } from './ToolTip';

const colorCSS = {
	blue: css`
		background-color: #588cdf;
	`,
	red: css`
		background-color: #ff7bc3;
	`,
	yellow: css`
		background-color: #ffb546;
	`,
	black: css`
		background-color: #000;
	`,
};

export const StyledTooltip = styled.div<TootipProps>`
	${({ backColor }) => colorCSS[backColor]};
	width: 27px;
	height: 27px;
	color: #fff;
	border: 1px solid transparent;
	border-radius: 50%;
	cursor: pointer;
	display: flex;
	justify-content: center;
	align-items: center;
`;