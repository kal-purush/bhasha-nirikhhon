export interface IButton {
    placeholder: string,
    onPress: () => void,
    style?: any,
    disable?: boolean,
    loading?: boolean
}