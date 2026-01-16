import { Size } from '@/components/DateText/types';

import { Theme } from '@emotion/react';
import styled from '@emotion/styled';

const generateTypography = (size: Size, theme: Theme) => {
  switch (size) {
    case 'small':
      return theme.typography.body3;
    case 'large':
      return theme.typography.body1;
  }
};

export const S = {
  DateText: styled.p<{ $size: Size }>`
    color: ${({ theme }) => theme.colors.gray[500]};

    ${({ theme, $size }) => generateTypography($size, theme)}
  `,
};