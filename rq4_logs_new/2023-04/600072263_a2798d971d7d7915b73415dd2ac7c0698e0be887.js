import React, { useState } from 'react';
import { View, Text, TouchableOpacity } from 'react-native';
import moment from 'moment';

const CustomCalendar = ({ onDayPress=()=>{} }) => {
    const [selectedDate, setSelectedDate] = useState(moment().format('YYYY-MM-DD'));
    const month = moment(selectedDate).startOf('month');
    const weeks = [];
    let currentWeek = [];
    for (let i = 0; i < month.daysInMonth(); i++) {
        const date = moment(month).add(i, 'days');
        currentWeek.push(date);
        if (date.day() === 6 || i === month.daysInMonth() - 1) {
            weeks.push(currentWeek);
            currentWeek = [];
        }
    }

    const handleDayPress = (date) => {
        setSelectedDate(date.format('YYYY-MM-DD'));
        onDayPress(date);
    };

    const renderDay = (date) => {
        const isSelected = date.format('YYYY-MM-DD') === selectedDate;
        const isToday = date.format('YYYY-MM-DD') === moment().format('YYYY-MM-DD');
        return (
            <TouchableOpacity
                style={[styles.dayContainer, isSelected && styles.selectedDayContainer]}
                onPress={() => handleDayPress(date)}
                key={date.format('YYYY-MM-DD')}
            >
                <Text style={[styles.dayText, isSelected && styles.selectedDayText, isToday && styles.todayText]}>
                    {date.date()}
                </Text>
            </TouchableOpacity>
        );
    };

    return (
        <View style={styles.container}>
            <View style={styles.weekContainer}>
                {['Su','Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'].map((day) => (
                    <View key={day} style={styles.weekdayContainer}>
                        <Text style={styles.weekdayText}>{day}</Text>
                    </View>
                ))}
            </View>
            {weeks.map((week) => (
                <View key={week[0].format('YYYY-MM-DD')} style={styles.weekContainer}>
                    {week.map((date) => renderDay(date))}
                </View>
            ))}
        </View>
    );
};

export default CustomCalendar;

import { StyleSheet } from 'react-native';
import {dark_cyan} from "./styles";

const styles = StyleSheet.create({
    container: {
        backgroundColor: 'white',
        borderRadius: 8,
        shadowColor: '#000',
        shadowOffset: {
            width: 0,
            height: 2,
        },
        shadowOpacity: 0.25,
        shadowRadius: 3.84,
        elevation: 5,
        paddingHorizontal: 16,
        paddingVertical: 16,
    },
    monthContainer: {
        alignItems: 'center',
        marginBottom: 16,
    },
    monthText: {
        fontWeight: 'bold',
        fontSize: 24,
    },
    weekContainer: {
        flexDirection: 'row',
        justifyContent: 'space-between',
        marginBottom: 0,
    },
    weekdayContainer: {
        width: 32,
        height: 32,
        alignItems: 'center',
        justifyContent: 'center',
    },
    weekdayText: {
        fontSize: 16,
    },
    dayContainer: {
        width: 32,
        height: 32,
        alignItems: 'center',
        justifyContent: 'center',
    },
    dayText: {
        fontSize: 16,
    },
    selectedDayContainer: {
        backgroundColor: '#1be08d',
        borderRadius: 8,
    },
    selectedDayText: {
        color: 'white',
    },
    todayText: {
        color: dark_cyan,
    },
});