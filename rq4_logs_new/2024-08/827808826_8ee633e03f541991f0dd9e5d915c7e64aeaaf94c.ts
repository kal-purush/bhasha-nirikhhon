import { Test, TestingModule } from '@nestjs/testing';
import { PeopleService } from '../People/people.service';

describe('PeopleService', () => {
  let service: PeopleService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PeopleService],
    }).compile();

    service = module.get<PeopleService>(PeopleService);
  });

  test('should return a new Client Account', () => {
    const account = service.createClient("Mariana","76523498765")
    expect(account).toHaveProperty("_name","Mariana");
    expect(account).toHaveProperty("_cpf","76523498765");
  });
  test('should find a Client Account by ID', () => {
    const account = service.findClientById("5790402b-5faf-4990-8938-a8b38422e9f0")
    expect(account).toHaveProperty("_peopleID","5790402b-5faf-4990-8938-a8b38422e9f0") 
  });
  test('should update a Client Account by ID', () => {
    const account = service.updateClient("5790402b-5faf-4990-8938-a8b38422e9f0","Luis","32873298768")
    expect(account).toHaveProperty("_peopleID","5790402b-5faf-4990-8938-a8b38422e9f0") 
    expect(account).toHaveProperty("_name","Luis") 
    expect(account).toHaveProperty("_cpf","32873298768")
  });
  test('deactivate a Client Account by ID', () => {
    const account = service.deactivateClientById("5790402b-5faf-4990-8938-a8b38422e9f0")
    expect(account).toHaveProperty("_peopleID","5790402b-5faf-4990-8938-a8b38422e9f0") 
    expect(account).toHaveProperty("_isActive",false) 
  });

  test('should return a new Bank Manager', () => {
    const account = service.createBankManager("Luisa","75421598547")
    expect(account).toHaveProperty("_name","Luisa");
    expect(account).toHaveProperty("_cpf","75421598547");
  });
  test('should find a bank Manager by ID', () => {
    const account = service.findBankManagerById("3a665d0e-8775-49ea-bc83-b72d6deb9b48")
    expect(account).toHaveProperty("_peopleID","3a665d0e-8775-49ea-bc83-b72d6deb9b48") 
  });
  test('should update a bank manager by ID', () => {
    const account = service.updateBankManager("3a665d0e-8775-49ea-bc83-b72d6deb9b48","Ana Maria","87534598765")
    expect(account).toHaveProperty("_peopleID","3a665d0e-8775-49ea-bc83-b72d6deb9b48") 
    expect(account).toHaveProperty("_name","Ana Maria") 
    expect(account).toHaveProperty("_cpf","87534598765")
  });
  test('deactivate a bank Manager by ID', () => {
    const account = service.deactivateBankManagertById("3a665d0e-8775-49ea-bc83-b72d6deb9b48")
    expect(account).toHaveProperty("_peopleID","3a665d0e-8775-49ea-bc83-b72d6deb9b48") 
    expect(account).toHaveProperty("_isActive",false) 
  });
});