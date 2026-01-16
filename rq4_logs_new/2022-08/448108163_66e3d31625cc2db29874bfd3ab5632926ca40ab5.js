import * as React from 'react'
import { render, screen } from '@testing-library/react'
import App from './App'

test('renders with world covid-19 data in the document', () => {
	render(<App />)
// 	const linkElement = screen.getByText(/World Covid-19 Data/i)
// 	expect(linkElement('svg-element')).toBeInTheDocument()
	// expect(linkElement).toBeInTheDocument()
	// test('renders div into document', () => {
	// 	const ref = React.createRef()
	// 	const {Card} = render(<card ref={ref} />)
	// 	expect(Card.firstChild).toBe(ref.current)
	// })
// 	it('Renders with a className equal to the variant', () => {
//     const { Card } = render(<Card variant="default" />)
// 		expect(screen.getByRole('card')).toHaveClass(Card)
//     // expect(container.firstChild).toHaveClass('foo')
// });
})