import { Consumer, Producer } from "kafkajs";

export interface InKafka {
    consumer?(topic: string | RegExp): Promise<void>
    producer?(topic: string, key: string, value: string): Promise<void>
    disconnect(servico: Consumer | Producer)
}