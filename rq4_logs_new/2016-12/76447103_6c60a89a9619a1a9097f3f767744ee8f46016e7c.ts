declare module 'request-promise' {
  function request<T>(options: any): Promise<T>
  export default request
}