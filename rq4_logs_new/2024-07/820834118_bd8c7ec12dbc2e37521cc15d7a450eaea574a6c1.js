
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

i18n
    .use(initReactI18next)
    .init({
        resources: {
            en: {
                translation: {

                    "total": "Total balance",
                    "expensesDay": "Expenses per Day - Week",
                    "week": {
                        "monday": "Mon",
                        "tuesday": "Tue",
                        "wednesday": "Wed",
                        "thursday": "Thu",
                        "friday": "Fri",
                        "saturday": "Sat",
                        "sunday": "Sun"
                    },
                    "todayExpenses": "Today expenses",
                    "comparation": "compared to yesterday"

                },
            },
            es: {
                translation: {
                    "total": "Balance total",
                    "expensesDay": "Gastos por día - Semana",
                    "week": {
                        "monday": "Lun",
                        "tuesday": "Mar",
                        "wednesday": "Mié",
                        "thursday": "Jue",
                        "friday": "Vie",
                        "saturday": "Sáb",
                        "sunday": "Dom"
                    },
                    "todayExpenses": "Gastos de hoy",
                    "comparation": "respecto a ayer"
                },
            },
            ca: {
                translation: {
                    "total": "Balanç total",
                    "expensesDay": "Despeses per dia - Setmana",
                    "week": {
                        "monday": "Dl",
                        "tuesday": "Dm",
                        "wednesday": "Dc",
                        "thursday": "Dj",
                        "friday": "Dv",
                        "saturday": "Ds",
                        "sunday": "Dg"
                    },
                    "todayExpenses": "Despeses avui",
                    "comparation": "respecte a ahir"
                },
            },
        },
        lng: 'en', 
        fallbackLng: 'en', 
        interpolation: {
            escapeValue: false, 
        },
    });

export default i18n;