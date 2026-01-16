import { ReadProductsController } from '../../presentation/controller/read-products/read-products';
import { ReadProductsAdapter } from '../../data/useCases/read-products/read-products-adapter';
import { ReadProductsRepositoryAdapter } from '../../data/repositories/read-products/read-products';
import { Mockend } from '../../utils/mockend-utils';

export const makeReadProductsController = (): ReadProductsController => {
  const mockend = new Mockend();
  const readProductsRepository = new ReadProductsRepositoryAdapter(mockend);
  const readProducts = new ReadProductsAdapter(readProductsRepository);
  return new ReadProductsController(readProducts);
}