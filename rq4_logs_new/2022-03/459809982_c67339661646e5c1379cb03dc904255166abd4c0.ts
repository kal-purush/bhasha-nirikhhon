import type { spacingFoundations } from '../../styles';

export interface PaddingStyleProps {
    padding?: keyof typeof spacingFoundations | number;
    paddingTop?: keyof typeof spacingFoundations | number;
    paddingBottom?: keyof typeof spacingFoundations | number;
    paddingLeft?: keyof typeof spacingFoundations | number;
    paddingRight?: keyof typeof spacingFoundations | number;
    paddingHorizontal?: keyof typeof spacingFoundations | number;
    paddingVertical?: keyof typeof spacingFoundations | number;
    paddingStart?: keyof typeof spacingFoundations | number;
    paddingEnd?: keyof typeof spacingFoundations | number;
}

export interface PaddingProps extends PaddingStyleProps {
    children: React.ReactNode;
}