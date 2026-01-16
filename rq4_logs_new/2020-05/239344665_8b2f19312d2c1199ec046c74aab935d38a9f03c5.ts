import { Cargo, CargoCollectionAuthorization, Message, Parcel } from '@relaycorp/relaynet-core';
import bufferToArray from 'buffer-to-arraybuffer';
import { buffer as getStdin } from 'get-stdin';

const RAMF_DESERIALIZER_BY_TYPE_OCTET: {
  readonly [key: number]: (s: ArrayBuffer) => Promise<Message<any>>;
} = {
  0x43: Cargo.deserialize,
  0x44: CargoCollectionAuthorization.deserialize,
  0x50: Parcel.deserialize,
};

export const command = 'deserialize';

export const description = 'Deserialize a message';

export const builder = {};

interface ArgumentSet {}

interface Deserialization {
  readonly type: 'Cargo' | 'CCA' | 'Parcel';
  readonly version: number;
  readonly recipientAddress: string;
  readonly id: string;
  readonly creationDate: Date;
  readonly ttl: number;
  readonly payload: string;
  readonly senderCertificate: string;
  readonly senderCertificateChain: readonly string[];
  readonly validationError: string | null;
}

export async function handler(_argv: ArgumentSet): Promise<void> {
  const ramfMessageSerialized = await getStdin();

  if (ramfMessageSerialized.length < 10) {
    throw Error('Input is too short to be a RAMF message');
  }

  const concreteMessageTypeOctet = ramfMessageSerialized[8];
  const deserializer = RAMF_DESERIALIZER_BY_TYPE_OCTET[concreteMessageTypeOctet];
  const ramfMessage = (await deserializer(bufferToArray(ramfMessageSerialized))) as Message<any>;
  const deserialization: Deserialization = {
    creationDate: ramfMessage.date,
    id: ramfMessage.id,
    payload: ramfMessage.payloadSerialized.toString('base64'),
    recipientAddress: ramfMessage.recipientAddress,
    senderCertificate: Buffer.from(ramfMessage.senderCertificate.serialize()).toString('base64'),
    senderCertificateChain: ramfMessage.senderCaCertificateChain.map(c =>
      Buffer.from(c.serialize()).toString('base64'),
    ),
    ttl: ramfMessage.ttl,
    type: ramfMessage.constructor.name as any,
    validationError: await getErrorMessage(() => ramfMessage.validate()),
    version: 0,
  };
  process.stdout.write(JSON.stringify(deserialization, null, 2));
}

async function getErrorMessage(func: () => Promise<any>): Promise<null | string> {
  try {
    await func();
  } catch (error) {
    return error.message;
  }
  return null;
}