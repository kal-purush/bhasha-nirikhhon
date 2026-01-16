export const clearFormsInSavings = () => ({
	type: 'CLEAR_FORMS_IN_SAVINGS'
})

export const changePLNFormInSavings = value => ({
	type: 'CHANGE_PLN_FORM_IN_SAVINGS',
	payload: value
})

export const changeTransferFormInSavings = value => ({
	type: 'CHANGE_TRANSFER_FORM_IN_SAVINGS',
	payload: value
})

export const changeSavings = value => ({
	type: 'CHANGE_SAVINGS',
	payload: Number(value)
})