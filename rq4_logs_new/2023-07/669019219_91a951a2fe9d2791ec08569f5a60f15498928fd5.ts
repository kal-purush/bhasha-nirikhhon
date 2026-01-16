import { useEffect, useState } from 'react';
import useLocalStorage from 'use-local-storage';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let recognition: any = null;
if ('webkitSpeechRecognition' in window) {
	recognition = new webkitSpeechRecognition();
	recognition.continuous = true;
	recognition.lang = 'ru' || 'en-US';
}

export const useSpeechRecording = () => {
	const [text, setText] = useState('')
	const [isRecording, setIsRecording] = useState(false)
	const [newQuestion, setNewQuestion] = useState('');

	useEffect(() => {
		if (!recognition) return;

		recognition.onresult = (event: SpeechRecognitionEvent) => {
			setNewQuestion(event.results[0][0].transcript)
		};
		recognition.stop();
		setIsRecording(false);
	}, []);

	const startRecording = () => {
		setNewQuestion('');
		setIsRecording(true);
		recognition.start();
	};

	const stopRecording = () => {
		setIsRecording(false);
		recognition.stop();
	};

	return {
		text,
		isRecording,
		startRecording,
		stopRecording,
		hasRecognitionSupport: !!recognition,
		newQuestion, 
		setNewQuestion,
	};
};