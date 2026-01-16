

import styled from 'styled-components';
import { motion } from 'framer-motion';

export const CartItemImage = styled(motion.img)`
    width: 112px;
    height: 112px;
    background-color: #E8F0D6;
    border-radius: 5px;
    border: 2px solid #E8F0D6;
`;

export const CartItemTitle = styled(motion.p)`
    font-family: 'Inter';
    font-style: normal;
    font-weight: 500;
    font-size: 18px;
    line-height: 28px;
	color: #161D25;
`;

export const CartItemCategoryTitle = styled(motion.p)<{color?: string; fontSize?: string}>`
	font-family: 'Inter';
	font-style: normal;
	font-weight: 400;
	font-size: ${(props) => props.fontSize? props.fontSize: '14px'};
	line-height: 22px;
	color: ${(props) => props.color? props.color: '#959EAD'};
`;