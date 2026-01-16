class BlankFieldError extends Error{
	constructor(token, username, password) {
		super()
		this.fields = [token, username, password]
	}

	token() {
		return this.fields[0]
	}

	username() {
		return this.fields[1]
	}

	password() {
		return this.fields[2]
	}
}
export default BlankFieldError;