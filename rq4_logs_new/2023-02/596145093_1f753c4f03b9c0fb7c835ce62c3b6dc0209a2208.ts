import { CreateBudget, CreateBudgetData } from "../../domain/useCases/create-budget";
import { ReadProducts } from "../../domain/useCases/read-products";
import { ReadUsers } from "../../domain/useCases/read-users";

export class CreateBudgetAdapter implements CreateBudget {
  constructor(
    private readUsers: ReadUsers,
    private readProducts: ReadProducts
  ) {}

  private async verifyProducts(productsId: number[]): Promise<string | void> {
    const products = await this.readProducts.read();
    const ids = products.map((product) => product.id);
    const exists = productsId.some((id) => ids.indexOf(id) === -1);

    if (exists === true) {
      return "Product"
    }
  }

  private async verifyUser(userId: number): Promise<string | void> {
    const users = await this.readUsers.read();
    const exists = users.some((user) => user.id === userId);
    
    if (exists === false) {
      return "User"
    }
  }

  create(data: CreateBudgetData): Promise<number | string> {
    throw new Error("Method not implemented.");
  }
}