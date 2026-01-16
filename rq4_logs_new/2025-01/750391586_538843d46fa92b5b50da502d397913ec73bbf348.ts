import 'server-only'

import { DirectAnswerPrompt } from '@/lib/llm/prompt'
import { LanguageModel, streamText } from 'ai'
import util from 'util'
import { StreamHandler, TextSource } from '@/lib/bizTypes'

export async function directlyAnswer(
  history: string,
  model: LanguageModel,
  query: string,
  searchContexts: TextSource[],
  onStream: StreamHandler,
  onError: (error: string) => void
) {
  try {
    const system = promptFormatterAnswer(searchContexts, history)
    // console.log('system prompt: ', system);

    const result = await streamText({
      model: model,
      system: system,
      prompt: query,
      maxRetries: 0,
      temperature: 0.1,
    })

    for await (const text of result.textStream) {
      onStream?.(text, false)
    }
  } catch (error) {
    console.log(error)
  }
}

function promptFormatterAnswer(searchContexts: any[], history: string) {
  const context = searchContexts
    .map((item, index) => `[citation:${index + 1}] ${item.content}`)
    .join('\n\n')

  return util.format(DirectAnswerPrompt, '', context, history)
}