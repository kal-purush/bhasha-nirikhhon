import { FlatList, FlatListProps } from 'react-native';
import styled from 'styled-components/native';
import { Typography } from '../../components/Typography';
import { MenuData, ItemData } from '../../services/menu/interface';

export const VerticalFlatlist = styled(
  FlatList as new (props: FlatListProps<MenuData>) => FlatList<MenuData>,
).attrs(({ theme }) => ({
  contentContainerStyle: {
    paddingTop: theme.spacingSize.spacing24,
  },
}))(({ theme }) => ({
  backgroundColor: theme.color.neutralWhite,
}));

export const HorizontalFlatlist = styled(
  FlatList as new (props: FlatListProps<ItemData>) => FlatList<ItemData>,
).attrs(({ theme }) => ({
  contentContainerStyle: {
    paddingHorizontal: theme.spacingSize.spacing20,
    paddingBottom: theme.spacingSize.spacing36,
  },
}))(({ theme }) => ({
  backgroundColor: theme.color.neutralWhite,
}));

export const ItemSeparator = styled.View(({ theme }) => ({
  width: theme.spacingSize.spacing16,
}));

export const Title = styled(Typography).attrs({
  size: 'titleXLarge',
  lineHeight: 'titleXLarge',
  weight: 'bold',
  fontFamily: 'InterBold',
})(({ theme }) => ({
  backgroundColor: theme.color.neutralWhite,
  paddingHorizontal: theme.spacingSize.spacing20,
  paddingBottom: theme.spacingSize.spacing20,
}));

export const FoodCard = styled.View.attrs({
  alignItems: 'center',
  justifyContent: 'center',
})(({ theme }) => ({
  width: 160,
  height: 160,
  padding: theme.spacingSize.spacing20,
  borderWidth: 1,
  borderRadius: 5,
  borderColor: theme.color.neutral20,
}));

export const FoodImage = styled.Image.attrs({
  resizeMode: 'contain',
})({
  width: 100,
  height: 100,
});

export const FoodName = styled(Typography).attrs({
  size: 'paragraphXSmall',
  lineHeight: 'paragraphXSmall',
  weight: 'regular',
  fontFamily: 'InterRegular',
})({
  textAlign: 'center',
});