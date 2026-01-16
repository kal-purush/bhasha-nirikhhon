/**
 * @author rgorai
 * @description details of a web error
 * @param status http status code
 * @param statusText text describing the status code
 * @param data any associated error data
 */
type ServerError = {
  status: string
  statusText: string
  data?: string
}