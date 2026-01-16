import { FlattenSimpleInterpolation } from 'styled-components';
import styled from 'styled-components/native';

import { COLOR } from '@theme/colors';

export const LayoutMainSectionStyled = {
  MainWrapper: styled.View<{ cssStyles?: FlattenSimpleInterpolation }>`
    flex: 1;
    width: 100%;
    padding-horizontal: 24px;
    padding-top: 24px;
    padding-bottom: 40px;
    background-color: ${COLOR.background.white};
    border-top-left-radius: 18px;
    border-top-right-radius: 18px;

    ${({ cssStyles }) => cssStyles}
  `,
};