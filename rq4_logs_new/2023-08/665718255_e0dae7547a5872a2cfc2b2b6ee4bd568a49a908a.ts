export interface ChatMessageProps {
  time: string;
  message: string;
  loading: boolean;
  id: string;
  isResponse: boolean;
  timeout: number;
}

export type ChatResponse = {
  propertyInfo: Property;
  response: string;
};

export type QueryMessage = {
  message: string;
};

export class Message implements ChatMessageProps {
  time: string;

  message: string;

  loading: boolean;

  id: string;

  timeout: number;

  isResponse: boolean;

  constructor({
    message,
    loading = false,
    timeout = 0,
    isResponse = false,
  }: Partial<ChatMessageProps>) {
    this.isResponse = isResponse;
    this.message = loading ? '' : message || '';
    this.timeout = timeout;
    this.loading = loading;
    const datetime = new Date();
    this.time = `${datetime.toLocaleDateString(
      'en-US',
    )} - ${datetime.getHours()}:${datetime.getMinutes()}`;
    this.id = Math.random().toString();
  }
}