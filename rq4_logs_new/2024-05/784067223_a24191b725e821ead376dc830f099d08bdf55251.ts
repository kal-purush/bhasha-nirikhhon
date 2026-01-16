import { sendToBackground } from '@plasmohq/messaging'
import { type GetConfigurationMessageResponsePayload } from '../../background/messages/getConfiguration'
import {
  type GetDocumentPreviewMessageRequestPayload,
  type GetDocumentPreviewMessageResponsePayload
} from '../../background/messages/getDocumentPreview'
import type { PairMessageRequestPayload, PairMessageResponsePayload } from '../../background/messages/pair'
import { type UnpairMessageResponsePayload } from '../../background/messages/unpair'
import { type UploadMessageRequestPayload, type UploadMessageResponsePayload } from '../../background/messages/upload'

export default class MessageManager {
  static MessageManager = new MessageManager()

  static async send (name: string, body: Record<string, string> = {}): Promise<unknown> {
    return await this.MessageManager.send(name, body)
  }

  static async sendGetDocumentPreviewMessage (url: string): Promise<GetDocumentPreviewMessageResponsePayload> {
    return await this.MessageManager.sendGetDocumentPreviewMessage(url)
  }

  static async sendGetConfigurationMessage (): Promise<GetConfigurationMessageResponsePayload> {
    return await this.MessageManager.sendGetConfigurationMessage()
  }

  static async sendPairMessage (oneTimeCode: string): Promise<PairMessageResponsePayload> {
    return await this.MessageManager.sendPairMessage(oneTimeCode)
  }

  static async sendUnpairMessage (): Promise<UnpairMessageResponsePayload> {
    return await this.MessageManager.sendUnpairMessage()
  }

  static async sendUploadMessage (fileName: string, url: string): Promise<UploadMessageResponsePayload> {
    return await this.MessageManager.sendUploadMessage(fileName, url)
  }

  readonly #extensionId: string

  constructor () {
    this.#extensionId = 'egdpalgnbmgehpebjmkklcfkggadmglp'
  }

  async sendGetDocumentPreviewMessage (url: string): Promise<GetDocumentPreviewMessageResponsePayload> {
    const body = { url } satisfies GetDocumentPreviewMessageRequestPayload
    return (await this.send('getDocumentPreview', body)) as GetDocumentPreviewMessageResponsePayload
  }

  async sendGetConfigurationMessage (): Promise<GetConfigurationMessageResponsePayload> {
    return (await this.send('getConfiguration')) as GetConfigurationMessageResponsePayload
  }

  async sendPairMessage (oneTimeCode: string): Promise<PairMessageResponsePayload> {
    const body = { oneTimeCode } satisfies PairMessageRequestPayload
    return (await this.send('pair', body)) as PairMessageResponsePayload
  }

  async sendUnpairMessage (): Promise<UnpairMessageResponsePayload> {
    return (await this.send('unpair')) as UnpairMessageResponsePayload
  }

  async sendUploadMessage (fileName: string, url: string): Promise<UploadMessageResponsePayload> {
    const body = { name: fileName, webDocumentUrl: url } satisfies UploadMessageRequestPayload
    return (await this.send('upload', body)) as UploadMessageResponsePayload
  }

  async send (name: string, body: Record<string, string> = {}): Promise<unknown> {
    // @ts-expect-error - Expected Error
    return await sendToBackground({ name, body, extensionId: this.#extensionId })
  }
}