import { jsonDataMock } from "../api/mockData"

const mockResponse = {
  data: jsonDataMock,
}
export default {
  get: jest.fn().mockResolvedValue(mockResponse),
}