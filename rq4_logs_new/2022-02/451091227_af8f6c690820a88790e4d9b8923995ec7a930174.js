export const convertDate = {
	methods: {
		convertDate(value) {
			return new Date(value).toLocaleDateString()
		}
	}
}