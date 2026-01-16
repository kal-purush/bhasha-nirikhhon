interface responseType<T> {
    message: string
    results: T
    success: boolean
}

export default responseType