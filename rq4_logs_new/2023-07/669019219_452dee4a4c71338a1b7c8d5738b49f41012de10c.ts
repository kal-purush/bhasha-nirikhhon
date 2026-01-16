import { Configuration, OpenAIApi } from 'openai';
import { Dispatch, SetStateAction, useState } from 'react';
import { TStoredValues } from '../components/AnswerSection/AnswerSection';

export const useOpenai = () => {
	const [storedValues, setStoredValues] = useState<Array<TStoredValues>>([]);
	const [status, setStatus] = useState(0)

	const configuration = new Configuration({
		apiKey: import.meta.env.VITE_REACT_APP_OPENAI_API_KEY,
	});

	const openai = new OpenAIApi(configuration);

	const generateResponse = async (
		newQuestion: string, 
		setNewQuestion: Dispatch<SetStateAction<string>>,
	) => {
		const options = {
			model: 'text-davinci-003',
			temperature: 0.7,
			max_tokens: 500,
			top_p: 1,
			frequency_penalty: 0.0,
			presence_penalty: 0.0,
			stop: ['/'],
		};

		const completeOptions = {
			...options,
			prompt: newQuestion,
		};
		const response = await openai.createCompletion(completeOptions);

		setStatus(response.status)

		if (response.data.choices) {
			setStoredValues([
				{
					question: newQuestion,
					answer: response.data.choices[0].text,
				},
				...storedValues,
			]);
			setNewQuestion('');
		}
	}
	return { storedValues, generateResponse, status }
}