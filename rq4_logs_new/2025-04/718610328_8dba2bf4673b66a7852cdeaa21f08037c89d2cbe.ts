import type { APIRoute } from 'astro'

export const prerender = false

export const GET: APIRoute = async ({ request }) => {
	return new Response('Method Not Allowed', { status: 405 })
}

export const POST: APIRoute = async ({ request }) => {
	try {
		const { email } = await request.json()
		// Listmonk API endpoint and credentials
		const listmonkUrl = 'https://listmonk.mukucodes.com/api/subscribers'
		const LISTMONK_API_KEY = import.meta.env.LISTMONK_API_KEY ?? process.env.LISTMONK_API_KEY
		// Just a simple check to make sure the API key is defined in an environment variable
		if (!LISTMONK_API_KEY) {
			console.error('No LISTMONK_API_KEY defined')
			return new Response(null, { status: 400 })
		}
		// Set headers for authentication
		const headers = {
			'Content-Type': 'application/json',
			Authorization: `token ${LISTMONK_API_KEY}`
		}

		// Data to be sent to Listmonk
		const data = {
			email,
			name: '' // Optional, can be left empty if not needed
		}

		// Make POST request to Listmonk API
		const response = await fetch(listmonkUrl, {
			method: 'POST',
			headers,
			body: JSON.stringify(data)
		})
		console.log('Response from Listmonk:', response.status)
		if (response.ok && response.status === 200) {
			console.log('Email subscribed:', email)
			return new Response(JSON.stringify({ message: 'Subscribed successfully' }), {
				status: 200,
				headers: {
					'Content-Type': 'application/json'
				}
			})
		} else if (response.status === 409) {
			console.log('Email already subscribed:', email)
			return new Response(JSON.stringify({ message: 'Already Subscribed!!' }), {
				status: 200,
				headers: {
					'Content-Type': 'application/json'
				}
			})
		} else {
			throw new Error(`Failed to subscribe: ${response.statusText}`)
		}
	} catch (error) {
		console.error('Error subscribing:', error)
		return new Response(JSON.stringify({ message: 'Error subscribing' }), {
			status: 500,
			headers: {
				'Content-Type': 'application/json'
			}
		})
	}
}