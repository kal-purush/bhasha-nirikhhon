import type { NativeStackNavigationOptions } from '@react-navigation/native-stack';

import { color, hexToRGBA } from '../styles';

/* eslint-disable import/prefer-default-export */
export const modalScreenOptions: NativeStackNavigationOptions = {
    presentation: 'modal',
    contentStyle: {
        backgroundColor: color('black', '90'),
    },
};

export const alertScreenOptions: NativeStackNavigationOptions = {
    presentation: 'modal',
    contentStyle: {
        backgroundColor: hexToRGBA(color('black') as string, 0.8),
    },
};