import { DocumentData } from 'firebase/firestore';
import { set } from 'lodash';

export function parseDocumentDates<T extends DocumentData>(data: T, parseDates: (keyof T)[]): T {
    const doc: T = { ...data };
    parseDates.forEach((dateField) => {
        const unparsedDate = doc?.[dateField];
        if (unparsedDate) {
            const parsedDate: Date | undefined = unparsedDate.toDate?.();
            if (parsedDate) {
                set(doc, dateField, parsedDate);
            }
        }
    });
    return doc;
}