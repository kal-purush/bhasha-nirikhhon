export interface OrderInterface {
  id: string;
  burger1: {
    name: string;
    quantity: number;
  };
  burger2: {
    name: string;
    quantity: number;
  };
  observation: string;
}