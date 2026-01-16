import { Kafka, KafkaConfig } from "kafkajs";
import dotenv from "dotenv";
dotenv.config();


interface KafkaEnvConfig {
  KAFKA_BROKERS: string;
  KAFKA_CLIENT_ID: string;
  KAFKA_GROUP_ID?: string;
  KAFKA_SSL_ENABLED?: boolean;
//   KAFKA_SSL_CA?: string;
//   KAFKA_SSL_KEY?: string;
//   KAFKA_SSL_CERT?: string;
}



function getKafkaConfig(): {
  clientConfig: KafkaConfig;
  groupId: string;
} {
  const {
    KAFKA_BROKERS,
    KAFKA_CLIENT_ID = "default-client-id", 
    KAFKA_GROUP_ID = "auth-group",
    KAFKA_SSL_ENABLED = "false",
    KAFKA_SSL_CA,
    KAFKA_SSL_KEY,
    KAFKA_SSL_CERT
  } = process.env;

  if (!KAFKA_BROKERS) {
    throw new Error("KAFKA_BROKERS  environment variable is required");
  }

  if (!KAFKA_CLIENT_ID) {
    throw new Error("KAFKA_CLIENT_ID environment variable is required");
  }

  const ssl = KAFKA_SSL_ENABLED === "true"
      ? {
          rejectUnauthorized: false,
          // ca: process.env.KAFKA_SSL_CA,
          // key: process.env.KAFKA_SSL_KEY,
          // cert: process.env.KAFKA_SSL_CERT
        }
      : undefined;

  return {
    clientConfig: {
      clientId: KAFKA_CLIENT_ID,
      brokers: KAFKA_BROKERS.split(","),
      ssl,
      connectionTimeout: 3000,
      requestTimeout: 25000,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    },
    groupId: KAFKA_GROUP_ID,
  };
}

export function createKafkaClient() {
  const { clientConfig } = getKafkaConfig();
  return new Kafka(clientConfig);
}

export function createKafkaProducer() {
  return createKafkaClient().producer();
}

export function createKafkaConsumer() {
    const { groupId } = getKafkaConfig();
    return createKafkaClient().consumer({ groupId, allowAutoTopicCreation: false });
}