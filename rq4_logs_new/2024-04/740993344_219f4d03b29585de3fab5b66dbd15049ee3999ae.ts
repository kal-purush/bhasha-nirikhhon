import { Alert, AlertButton } from 'react-native';

export const showAlert = (
  title: string,
  message: string,
  cancelable: boolean = false
): Promise<boolean> => {
  return new Promise(resolve => {
    const buttons: AlertButton[] = [
      { text: 'OK', onPress: () => resolve(true), style: 'default' },
    ];
    if (cancelable) {
        buttons.push({ text: 'Cancel', onPress: () => resolve(false), style: 'cancel' });
    }
    const options = { cancelable: cancelable };
    Alert.alert(title, message, buttons, options);
  });
};