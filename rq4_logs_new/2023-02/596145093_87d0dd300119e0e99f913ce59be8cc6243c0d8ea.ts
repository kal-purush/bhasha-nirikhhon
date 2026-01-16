import { CreateBudgetAdapter } from "../../data/useCases/create-budget/create-budget-adapter";
import { ReadUsersAdapter } from "../../data/useCases/read-users/read-users-adapter";
import { CreateBudgetController } from "../../presentation/controller/create-budget/create-budget";
import { ReadUsersRepositoryAdapter } from '../../data/repositories/read-users/read-users';
import { Mockend } from "../../utils/mockend-utils";
import { ReadProductsRepositoryAdapter } from '../../data/repositories/read-products/read-products';
import { ReadProductsAdapter } from '../../data/useCases/read-products/read-products-adapter';

export const makeCreateBudgetController = (): CreateBudgetController => {
  const mockend = new Mockend();
  const readUsersRepository = new ReadUsersRepositoryAdapter(mockend);
  const readUsersAdapter = new ReadUsersAdapter(readUsersRepository);
  const readProductsRepository = new ReadProductsRepositoryAdapter(mockend);
  const readProductsAdapter = new ReadProductsAdapter(readProductsRepository);
  const createBudget = new CreateBudgetAdapter(readUsersAdapter ,readProductsAdapter);
  return new CreateBudgetController(createBudget);
}