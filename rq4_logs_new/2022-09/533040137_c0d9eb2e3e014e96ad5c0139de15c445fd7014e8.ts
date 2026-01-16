import container from '@di/index';
import { ICustomer } from '@interfaces/domain/customer/repository';
import CustomerRepository from '../repository/CustomerRepository';
import CreateCustomerService from './CreateCustomerService';
import CustomerValidator from './helpers/CustomerValidator';

const newCustomer: ICustomer = {} as ICustomer;
describe('CreateCustomerService', () => {
  describe('create', () => {
    const createCustomerService = container.resolve(CreateCustomerService);
    const spyRepository = jest
      .spyOn(CustomerRepository.prototype, 'create')
      .mockReturnValue(newCustomer);
    const spyValidator = jest
      .spyOn(CustomerValidator.prototype, 'validate')
      .mockResolvedValue();
    it('Should resolve new user when input data is correct', async () => {
      expect(await createCustomerService.create(newCustomer)).toEqual(
        newCustomer,
      );
    });
    it('Should reject with error when validation fails', async () => {
      spyValidator.mockImplementationOnce(() => Promise.reject(new Error()));
      await expect(createCustomerService.create(newCustomer)).rejects.toEqual(
        new Error(),
      );
    });
    it('Should reject with error when repository fails', async () => {
      spyRepository.mockImplementation(() => {
        throw new Error();
      });
      await expect(createCustomerService.create(newCustomer)).rejects.toEqual(
        new Error(),
      );
    });
  });
});