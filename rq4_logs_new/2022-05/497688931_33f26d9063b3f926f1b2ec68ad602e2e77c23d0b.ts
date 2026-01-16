import React from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';


export type StorageContext = {
    money: number,
    setMoney: (money: number) => void,
    achievements: string[],
    setAchievements: (achievements: string[]) => void
};

const initialValue: StorageContext = {
    money: 0,
    setMoney: (money: number) =>
        AsyncStorage.setItem('money', money.toString()).then(() => {
            initialValue.money = money;
        }),

    achievements: [],
    setAchievements: (achievements: string[]) =>
        AsyncStorage.setItem('achievements', achievements.join()).then(() => {
            initialValue.achievements = achievements;
        })
};

AsyncStorage.getItem('money').then((v) => {
    initialValue.money = parseInt(v ?? '0');
});

AsyncStorage.getItem('achievements').then((v) => {
    initialValue.achievements = v?.split(',') ?? [];
});

const context = React.createContext(initialValue);


export default { initialValue, context };