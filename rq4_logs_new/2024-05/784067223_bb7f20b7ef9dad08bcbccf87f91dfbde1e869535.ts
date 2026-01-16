import type { PlasmoMessaging } from '@plasmohq/messaging'

import ExtensionManager from '../../lib/services/ExtensionManager'
import Message from '../../lib/utils/Message'

export interface GetExtensionMetadataResponsePayload {
  id: string
}

class GetExtensionMetadataMessage extends Message {
  protected async process (request: PlasmoMessaging.Request): Promise<GetExtensionMetadataResponsePayload> {
    const extensionManager = new ExtensionManager()
    return { id: extensionManager.extensionId }
  }
}

const handler: PlasmoMessaging.MessageHandler = async (request, response) => {
  const message = new GetExtensionMetadataMessage()
  await message.handle(request, response)
}

export default handler