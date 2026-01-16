import { useEffect, useState } from 'react'
import styled from 'styled-components'
import firebase from 'firebase/app'
import 'firebase/database'
import Testmonial from '../../Molecules/Testimonial'
import { MOBILE_WIDTH } from '../../constants'

const Container = styled.div`
	display: flex;
	flex-direction: column;
	align-items: center;
	margin: 30px 10vw;
	& > article {
		margin-bottom: 20px;
	}
	@media (max-width: ${MOBILE_WIDTH}) {
		margin: 30px;
	}
`

const Messages = () => {
	const [messages, setMessages] = useState([])
	if (!firebase.apps.length) {
		const firebaseConfig = require('../../firebaseConfig.json')
		firebase.initializeApp(firebaseConfig)
	}

	useEffect(() => {
		;(async () => {
			const postListRef = await firebase
				.database()
				.ref('messages')
				.once('value')
			const values = postListRef.val()
			setMessages(Object.values(values))
		})()
	}, [])

	if (!messages.length) {
		return <div>Loading...</div>
	}

	return (
		<Container>
			{messages.map((message, key) => (
				<Testmonial data={message} key={key} />
			))}
		</Container>
	)
}

export default Messages