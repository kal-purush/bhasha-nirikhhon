import {http} from "../helpers";
import {basePath} from "../config";
import {ICategory} from "../store/categories/types";

/*
 * service works with categories: add, remove, get
 *   */
class CategoriesService {

    async getCategoriesList():Promise<ICategory[]>{
        const responce:Array<Object> = await http({url:`${basePath}/category`}) ;
        const result:ICategory[] = responce as ICategory[];
        return result;
    }

    async addNew(newCategoryData: ICategory):Promise<ICategory> {
        const responce: ICategory = await http({
            url: `${basePath}/category`,
            init: {
                method: "post",
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(newCategoryData)
            }
        });
        return responce;
    }
/*
    async updateProduct(updatedProductData: IProduct):Promise<IProduct> {
        console.log("updatedProductData",updatedProductData);
        const responce: IProduct = await http({
            url: `${basePath}/product/${updatedProductData.id}`,
            init: {
                method: "PUT",
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(updatedProductData)
            }
        });
        return responce;
    }

    async removeProduct(product: IProduct):Promise<string> {
        const responce: string = await http({
            url: `${basePath}/product/${product.id}`,
            init: {
                method: "DELETE",
                headers: {'Content-Type': 'application/json'},
                body:JSON.stringify(product)
            }
        });
        return responce;
    }*/
}
export default new CategoriesService();