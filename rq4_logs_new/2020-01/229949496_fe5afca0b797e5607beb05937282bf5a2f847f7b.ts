const importHistoryKey = 'imoprt-history'
export const getImportHistory = (): string[] => {
    const history = localStorage.getItem(importHistoryKey);
    if (history === null) {
        return [];
    }
    return JSON.parse(history);
};

export const setImportHistory = (history: string[]) => {
    localStorage.setItem(importHistoryKey, JSON.stringify(history));
};

export const addImportHistory = (newHistoryURL: string, limit = 5): string[] => {
    const history = getImportHistory().filter((h) => h !== newHistoryURL);
    const newHistory = [newHistoryURL, ...history].slice(0,limit);
    setImportHistory(newHistory);
    return newHistory;
};

export const deleteImportHistory = (url: string): string[] => {
    const history = getImportHistory().filter((h) => h !== url);
    setImportHistory(history);
    return history;
};