import type { ViewStyle } from 'react-native';

export interface DateFieldProps {
    /**
     * The label of the field
     */
    label?: string;
    /**
     * The date value of the field
     */
    date?: string;
    /**
     * The event handler to open the date picker
     */
    onOpen?: () => void;
    /**
     * The styles object of the container
     */
    style?: ViewStyle;
    /**
     * Placeholder of the text input
     */
    placeholder?: string;
    /**
     * Sizes of the text input
     */
    size?: 'medium' | 'large';
    /**
     * The event handler to change the date value
     */
    onChange?: (date: Date) => void;
}