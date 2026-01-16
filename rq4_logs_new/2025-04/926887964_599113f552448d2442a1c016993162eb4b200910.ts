import { makeAutoObservable, runInAction } from 'mobx';
import { 
  fetchOrders, 
  fetchOrder, 
  createOrder, 
  updateOrder, 
  deleteOrder 
} from '../services/api';
import { Order, OrderCreate } from '../types';
import { RootStore } from './RootStore';

/**
 * MobX store for order management
 */
export class OrderStore {
  // Observable state
  orders: Order[] = [];
  currentOrder: Order | null = null;
  loading: boolean = false;
  error: string | null = null;
  
  // Root store reference
  rootStore: RootStore;

  constructor(rootStore: RootStore) {
    this.rootStore = rootStore;
    
    // Make all properties observable
    makeAutoObservable(this, {
      rootStore: false // Do not make the root store reference observable
    });
  }

  /**
   * Load all orders from the API
   */
  loadOrders = async (): Promise<void> => {
    this.setLoading(true);
    this.setError(null);
    
    try {
      const data = await fetchOrders();
      
      // Use runInAction to batch updates to observable state
      runInAction(() => {
        this.orders = data;
        this.loading = false;
      });
    } catch (err) {
      runInAction(() => {
        this.error = 'Failed to fetch orders. Please try again later.';
        this.loading = false;
      });
      console.error(err);
    }
  };

  /**
   * Load a specific order by ID
   */
  loadOrder = async (id: number): Promise<void> => {
    if (!id) return;
    
    this.setLoading(true);
    this.setError(null);
    
    try {
      const data = await fetchOrder(id);
      
      runInAction(() => {
        this.currentOrder = data;
        this.loading = false;
      });
    } catch (err) {
      runInAction(() => {
        this.error = `Failed to fetch order with ID ${id}. Please try again later.`;
        this.loading = false;
      });
      console.error(err);
    }
  };

  /**
   * Create a new order
   */
  createNewOrder = async (orderData: OrderCreate): Promise<Order | null> => {
    this.setLoading(true);
    this.setError(null);
    
    try {
      const newOrder = await createOrder(orderData);
      
      runInAction(() => {
        // Add the new order to our orders array
        this.orders = [...this.orders, newOrder];
        this.currentOrder = newOrder;
        this.loading = false;
      });
      
      return newOrder;
    } catch (err) {
      runInAction(() => {
        this.error = 'Failed to create order. Please try again later.';
        this.loading = false;
      });
      console.error(err);
      return null;
    }
  };

  /**
   * Update an existing order
   */
  updateExistingOrder = async (id: number, orderData: Order): Promise<Order | null> => {
    this.setLoading(true);
    this.setError(null);
    
    try {
      const updatedOrder = await updateOrder(id, orderData);
      
      runInAction(() => {
        // Update the order in our orders array
        this.orders = this.orders.map(order => 
          order.id === updatedOrder.id ? updatedOrder : order
        );
        
        // Update current order if it's the one being edited
        if (this.currentOrder && this.currentOrder.id === updatedOrder.id) {
          this.currentOrder = updatedOrder;
        }
        
        this.loading = false;
      });
      
      return updatedOrder;
    } catch (err) {
      runInAction(() => {
        this.error = `Failed to update order with ID ${id}. Please try again later.`;
        this.loading = false;
      });
      console.error(err);
      return null;
    }
  };

  /**
   * Delete an order
   */
  deleteExistingOrder = async (id: number): Promise<boolean> => {
    this.setLoading(true);
    this.setError(null);
    
    try {
      await deleteOrder(id);
      
      runInAction(() => {
        // Remove the order from our orders array
        this.orders = this.orders.filter(order => order.id !== id);
        
        // Reset current order if it's the one being deleted
        if (this.currentOrder && this.currentOrder.id === id) {
          this.currentOrder = null;
        }
        
        this.loading = false;
      });
      
      return true;
    } catch (err) {
      runInAction(() => {
        this.error = `Failed to delete order with ID ${id}. Please try again later.`;
        this.loading = false;
      });
      console.error(err);
      return false;
    }
  };

  /**
   * Set loading state
   */
  setLoading = (loading: boolean): void => {
    this.loading = loading;
  };

  /**
   * Set error message
   */
  setError = (error: string | null): void => {
    this.error = error;
  };

  /**
   * Reset the store to initial state
   */
  reset = (): void => {
    this.orders = [];
    this.currentOrder = null;
    this.loading = false;
    this.error = null;
  };
} 