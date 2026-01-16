import { MessageFormatElement } from 'intl-messageformat-parser';

interface LocalizationMessage {
  [key: string]: {} | string;
}

type MessagesProp = Record<string, string> | Record<string, MessageFormatElement[]>;

export function flattenMessages(nestedMessages: LocalizationMessage, prefix = ''): MessagesProp {
  return Object.keys(nestedMessages).reduce((messages: MessagesProp, key: string) => {
    const value = nestedMessages[key];
    const prefixKey = prefix ? `${prefix}.${key}` : key;

    if (typeof value === 'string') {
      messages[prefixKey] = value;
    } else {
      Object.assign(messages, flattenMessages(value, prefixKey));
    }

    return messages;
  }, {})
}