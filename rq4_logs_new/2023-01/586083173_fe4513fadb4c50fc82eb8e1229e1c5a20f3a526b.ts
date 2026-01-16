import { StyleProp, ViewStyle } from 'react-native';

export interface ListItemProps {
    icon?: JSX.Element;
    title: string;
    description?: string;
    hasSwitch?: boolean;
    toggleSwitch?: (value: boolean) => void;
    switchValue?: boolean;
    hasArrow?: boolean;
    onPress?: () => void;
    style?: StyleProp<ViewStyle>;
}

export const ListItemDefaultProps: Omit<ListItemProps, 'title'> = {
    icon: undefined,
    description: null,
    hasSwitch: false,
    toggleSwitch: (value: boolean) => {},
    switchValue: false,
    hasArrow: false,
    onPress: () => {}
};