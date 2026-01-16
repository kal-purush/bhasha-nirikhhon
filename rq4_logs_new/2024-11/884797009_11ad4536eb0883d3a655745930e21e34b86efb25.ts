"use server";

import OpenAI from 'openai';

const client = new OpenAI({
    apiKey: process.env['OPENAI_API_KEY'],
});

const createTranslationPrompt = (text: string, sourceLanguage: string, targetLanguages: string[]): string => {
    const validTargets = targetLanguages.filter(lang => lang !== sourceLanguage);

    return `TRANSLATION TASK

SOURCE TEXT (${sourceLanguage}):
${text}

NOTE: The source text might be written phonetically/transliterated (like "kaise ho" for Hindi/Urdu or "ni hao" for Chinese). 
Identify and understand such phonetic writings in the source language.

TRANSLATE TO ONLY:
${validTargets.map(lang => lang).join('\n')}

INSTRUCTIONS:
1. First understand the source text:
   - If it's phonetically written (like roman urdu, pinyin, etc.), interpret it correctly
   - Consider common phonetic spellings and variations
   - Understand informal transliterations

2. Then translate while:
   - Keeping the same tone and intention
   - Maintaining any informal style
   - Preserving formatting and punctuation
   - Keeping emojis and special characters

3. For each target language:
   - Translate into proper script (not phonetic)
   - Keep names and borrowed words as they should appear
   - Maintain equivalent level of formality

RESPONSE FORMAT:
${sourceLanguage}:
${text}

${validTargets.map(lang => `${lang}:
[translation in proper script]`).join('\n\n')}

IMPORTANT:
- Only translate to the requested languages
- Use proper script for translations (not phonetic)
- Don't add explanations or notes
- Don't add any other languages`;
}

export async function translateAction(
    text: string, 
    sourceLanguage: string, 
    targetLanguages: string[]
): Promise<Record<string, string>> {
    try {
        const uniqueTargets = [...new Set(
            targetLanguages.filter(lang => lang !== sourceLanguage)
        )];

        const prompt = createTranslationPrompt(text, sourceLanguage, uniqueTargets);

        const completion = await client.chat.completions.create({
            messages: [
                {
                    role: 'system',
                    content: `You are a precise translator with expertise in phonetic/transliterated text. 
                    You can understand text written in roman script but meant to be read in other languages 
                    (like "kya hal hai" for Urdu or "wo hen hao" for Chinese). 
                    Always translate into proper script of the target language.`
                },
                {
                    role: 'user',
                    content: prompt
                }
            ],
            model: 'gpt-3.5-turbo',
            temperature: 0.1
        });

        const response = completion.choices[0]?.message?.content;
        if (!response) {
            throw new Error('No response from OpenAI');
        }

        const translations = extractTranslations(response, sourceLanguage, text, uniqueTargets);
        
        // Verify we only have requested languages
        const finalTranslations: Record<string, string> = {
            [sourceLanguage]: text
        };

        // Only include requested languages
        for (const lang of uniqueTargets) {
            if (translations[lang]) {
                finalTranslations[lang] = translations[lang];
            }
        }

        return finalTranslations;

    } catch (error) {
        console.error('Translation error:', error);
        return {
            [sourceLanguage]: text
        };
    }
}

function extractTranslations(
    response: string,
    sourceLanguage: string,
    text: string,
    targetLanguages: string[]
): Record<string, string> {
    const translations: Record<string, string> = {
        [sourceLanguage]: text
    };

    // Create a set of requested languages for quick lookup
    const requestedLanguages = new Set([
        sourceLanguage,
        ...targetLanguages
    ]);

    // Split into sections
    const sections = response
        .split(/(?=^[A-Za-z]+:)/m)
        .map(section => section.trim())
        .filter(Boolean);

    for (const section of sections) {
        const lines = section.split('\n');
        if (lines.length < 1) continue;

        const firstLine = lines[0];
        if (!firstLine.includes(':')) continue;

        const [language, ...firstContent] = firstLine.split(':');
        const languageName = language.trim();

        // Only process if this is a requested language
        if (!requestedLanguages.has(languageName)) continue;

        // Skip if this is just repeating the source language
        if (languageName === sourceLanguage) continue;

        const content = [
            ...firstContent,
            ...lines.slice(1)
        ]
            .join('\n')
            .trim()
            // Clean quotes
            .replace(/^["']|["']$/g, '')
            // Remove template text
            .replace(/\[translation[^]]*\]/gi, '')
            // Clean markers
            .replace(/\s*END\s*$/i, '')
            // Normalize spaces
            .replace(/[ \t]+/g, ' ')
            .trim();

        if (content) {
            translations[languageName] = content;
        }
    }

    return translations;
}   