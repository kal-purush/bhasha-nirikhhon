import axios from "axios";
import CategoryDto from "./dto/categoryDto";

class CategoryService {
  public async getCategories() {
    let result = await axios.get(
      "http://localhost:5011/api/v1/Category/GetAll"
    );
    return result.data.data;
  }
}
export default new CategoryService();