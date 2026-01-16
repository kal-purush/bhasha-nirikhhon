import ArrayBufferConverter, { getBuffer } from '../Converter';

test('Тестируем метод load(). Если объекты совпадают - sucess', () => {
  const buffer = getBuffer();
  ArrayBufferConverter.load(buffer);
  expect(ArrayBufferConverter.buffer).toEqual(buffer);
});

test("Тестируем метод toString(). Если строки совпадают - sucess", () => {
  const buffer = getBuffer();
  ArrayBufferConverter.load(buffer);
  expect(ArrayBufferConverter.buffer).toEqual(buffer);
  const receivedString = ArrayBufferConverter.toString();
  const exempleString = '{"data":{"user":{"id":1,"name":"Hitman","level":10}}}';
  expect(receivedString).toBe(exempleString);
});