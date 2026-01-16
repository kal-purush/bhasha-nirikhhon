import IProduct = warehouse.IProduct;

export class Product implements warehouse.IProduct {
    title:string = 'New Product';
    private _weight:number = 0;
    children:IProduct[];
    
    constructor(data:{}) {
        Object.assign(this, data);
    }

    private calculateWeight() {

        return !this.children ?
            this.weight :
            _.map(this.children, 'weight').reduce((totalWeight:number, weight:number) => totalWeight + weight, 0) as number;
    }

    get weight():number {
        const weight:number = this.calculateWeight();

        return weight ? (this._weight = weight) : 0;
    }
    
    setWeight(newWeight:string | number):void {
        const weight:number = Number(newWeight);
        !Number.isNaN(weight) && (this._weight = weight);
    }
    
    setTitle(title:string) {
        this.title = title;
    }

    hasRelatedProducts():boolean {

        return !_.isEmpty(this.children);
    }
    
    addRelatedProduct(product:IProduct):this {
        !this.hasRelatedProducts() && (this.children = []);
        this.children.unshift(product);

        return this;
    }
}