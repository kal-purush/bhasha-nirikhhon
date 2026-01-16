export const modelConfigData = [
    { modelConfigId: 0, modelConfigName: 'gpt4', modelConfigLabel: 'OpenAI GPT-4', modelName: 'gpt4', temperature: 0.9, streaming: false, maxTokens: 256 },
    { modelConfigId: 1, modelConfigName: 'gpt35', modelConfigLabel: 'OpenAI GPT-3.5', modelName: 'gpt35', temperature: 0.9, streaming: false, maxTokens: 256 },
    { modelConfigId: 2, modelConfigName: 'llama270', modelConfigLabel: 'Meta Llama 2 70B (can be slow!)', modelName: 'llama270', temperature: 0.9, streaming: false, maxTokens: 256 }
];

export const languageOptionData = [
    { value: 'englishToSamoan', label: 'English to Samoan' },
    { value: 'samoanToEnglish', label: 'Samoan to English' },
    { value: 'englishToChamorro', label: 'English to Chamorro' },
    { value: 'chamorroToEnglish', label: 'Chamorro to English' }
];

// label is the language ISO-639-1 code, unless the language doesn't have one, then it's the ISO-639-2 code.
// if the language doesn't have the ISO-639-2 code, then it's the English name of the language.
export const languageKeys = [
    { value: 'English', label: 'En' },
    { value: 'Samoan', label: 'Sm' },
    { value: 'Chamorro', label: 'Ch' },
    { value: 'Carolinian', label: 'Cal' }
];