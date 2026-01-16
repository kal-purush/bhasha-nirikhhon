import styled from 'styled-components';
import { motion } from 'framer-motion';

export const ProductColumn = styled(motion.div)`
	display: flex;
	flex-flow: column;
	justify-content: center;
	align-items: center;
	background: #FFFFFF;
	padding: 10px;
	border: 1px solid #F1F1F1;
	border-radius: 8px;
`;

export const Row = styled(motion.div)`
	display: flex;
	flex-direction: row;
	flex-wrap: wrap;
	width: 100%;
	padding-left: 5%;
`;

export const Col = styled(motion.div)`
	display: flex;
	flex-direction: column;
	flex-basis: 100%;
	flex: 1;
`;

export const SaleButton = styled(motion.button)`
	padding: 5px;
	width: 49px;
	height: 24 px;
	background: #FADBD8;
	mix-blend-mode: normal;
	border: 1px solid #FADBD8;
	border-radius: 4px;
	font-family: 'Roboto';
	font-style: normal;
	font-weight: 500;
	font-size: 12px;
	line-height: 18px;
	text-align: center;
	color: #DE3618;
`;


