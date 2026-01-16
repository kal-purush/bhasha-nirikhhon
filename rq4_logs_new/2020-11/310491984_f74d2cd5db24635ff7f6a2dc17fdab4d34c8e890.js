import React from 'react'
import ContactForm from './ContactForm'
import { render, screen, fireEvent } from '@testing-library/react'

test('renders Contact form without crashing',  () => {
    render(<ContactForm />)
})


test("Testing Input Forms" , async () => {

  render(<ContactForm />)

    //testing first name field
    const name = screen.queryByLabelText(/First Name/i)
    // console.log(name)
    fireEvent.change(name, {target: {value:"Chad"}})
    expect(name).toHaveValue("Chad")

    //testing last name field
    const lastName = screen.getByLabelText(/Last Name/i)
    // console.log(lastName)
    fireEvent.change(lastName, {target: {value:"Diaz"}})
    expect(lastName).toHaveValue("Diaz")

    //testing email input field
    const emailInput= screen.getByPlaceholderText(/@/i)
    fireEvent.change(emailInput, {target: {value:"chad@gmail.com"}})
    expect(emailInput).toHaveValue('chad@gmail.com')

    //testing message input field
    const messageInput= screen.getByRole(/textbox/i)
    // console.log(messageInput)
    fireEvent.change(messageInput, {target: {value:"This is a test message. Hello."}})
    expect(messageInput).toHaveValue('This is a test message. Hello.')

    //testing the submit area / button
    const submit = screen.getByTestId(/submit/i)
    fireEvent.click(submit)
})