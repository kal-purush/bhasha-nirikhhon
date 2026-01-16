import { useSelector } from 'react-redux'
import { USER_LOGOS } from '../../utils/constants'

export const useLLMChatState = () => ({
	sideBarState: {
		variant: useSelector(state => state.LLMChatPage.variant),
		temperature: useSelector(state => state.LLMChatPage.sideBar.temperature),
		typingSpeed: useSelector(state => state.LLMChatPage.typingSpeed),
		userID: useSelector(state => state.LLMChatPage.userId),
		isSideBarOpen: useSelector(state => state.LLMChatPage.sideBar.isSideBarOpen),
		maxwidth: 260,
		buttonText: "New Chat",
		logoutText: "Log Out",
		exportText: "Export to Text",
		threadIndex: useSelector(state => state.LLMChatPage.threadIndex),
		threads: useSelector(state => state.LLMChatPage.sideBar.threads),
		threadListState: {
			variant: useSelector(state => state.LLMChatPage.variant),
			userId: useSelector(state => state.LLMChatPage.userId),
			maxwidth: 260,
			threads: useSelector(state => state.LLMChatPage.sideBar.threads),
		},
		chatHistory: useSelector(state => state.LLMChatPage.chatHistory),
	},
	chatListState: {
		variant: useSelector(state => state.LLMChatPage.variant),
		chatHistory: useSelector(state => state.LLMChatPage.chatHistory),
		userLogos: USER_LOGOS,
		typingSpeed: useSelector(state => state.LLMChatPage.typingSpeed),
		chatInputState: {
			variant: useSelector(state => state.LLMChatPage.variant),
			placeholder: 'Write a Message...',
			name: '',
			buttonType: 'submit',
			userID: useSelector(state => state.LLMChatPage.userId),
			userInput: useSelector(state => state.LLMChatPage.chatList.userInput),
			isNewChat: useSelector(state => state.LLMChatPage.chatList.isNewChat),
			threads: useSelector(state => state.LLMChatPage.sideBar.threads),
			threadIndex: useSelector(state => state.LLMChatPage.threadIndex),
			chatHistory: useSelector(state => state.LLMChatPage.chatHistory),
		},
	},
})