import faker from 'faker';
import { mockAddExercise } from '../../../domain/tests/mock-add-exercise';
import { AddExercise } from '../../../domain/usecases';
import { HttpPostClient } from '../../protocols/http';
import {  httpPostClientSpy, httpPostClientSpybody, httpPostClientSpyURl } from '../../test/mock-http';
import { remoteAddExercise } from './remote-add-exercise';

type SutTypes = {
    sut: AddExercise,
    httpPostClient: HttpPostClient
}
  
  const makeSut = (url: string = faker.internet.url()): SutTypes => {
    const httpPostClient = httpPostClientSpy();
    const sut = remoteAddExercise({url, httpClient: httpPostClient})
  
    return {
        sut,
        httpPostClient
    }
  }

describe('RemoteExercise', () => {
    it('should call HttpPostClient with correct URL', async () => {
      const url = faker.internet.url()
      const { sut } = makeSut(url)
      sut.add(mockAddExercise())
  
      expect(httpPostClientSpyURl).toBe(url)
    })

    it('should call HttpPostClient with correct Body', async () => {
      const url = faker.internet.url()
      const body = mockAddExercise()
      const { sut } = makeSut(url)
      sut.add(body)
      expect(httpPostClientSpybody).toBe(body)
    })
})