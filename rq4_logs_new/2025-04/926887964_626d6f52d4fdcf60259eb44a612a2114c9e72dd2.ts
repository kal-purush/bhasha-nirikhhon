import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { OrderStore } from './OrderStore';
import { RootStore } from './RootStore';
import * as apiService from '../services/api';

/**
 * Tests for the OrderStore
 * 
 * These tests verify the OrderStore's functionality for managing orders:
 * - Loading orders
 * - Creating orders
 * - Updating orders
 * - Deleting orders
 * - Error handling
 */
describe('OrderStore', () => {
  // Setup variables
  let rootStore: RootStore;
  let orderStore: OrderStore;
  
  // Mock data
  const mockOrders = [
    { 
      id: 1, 
      orderNumber: 'ORD-001', 
      status: 'pending',
      customerId: '12345',
      items: [{ productId: 101, quantity: 2 }],
      createdAt: '2023-01-01T00:00:00Z',
      updatedAt: '2023-01-01T00:00:00Z'
    },
    { 
      id: 2, 
      orderNumber: 'ORD-002', 
      status: 'shipped',
      customerId: '67890',
      items: [{ productId: 102, quantity: 1 }],
      createdAt: '2023-01-02T00:00:00Z',
      updatedAt: '2023-01-03T00:00:00Z'
    }
  ];
  
  // Mock single order
  const mockSingleOrder = { 
    id: 3, 
    orderNumber: 'ORD-003', 
    status: 'delivered',
    customerId: '13579',
    items: [{ productId: 103, quantity: 3 }],
    createdAt: '2023-01-04T00:00:00Z',
    updatedAt: '2023-01-05T00:00:00Z'
  };

  // Setup before each test
  beforeEach(() => {
    // Reset mocks
    vi.resetAllMocks();
    
    // Create new instances for each test
    rootStore = new RootStore();
    orderStore = new OrderStore(rootStore);
    
    // Mock API service functions - only mock what's necessary for each test
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test initial state of the OrderStore
   */
  it('should initialize with default values', () => {
    // Arrange - store is already initialized in beforeEach
    
    // Act - no action needed
    
    // Assert
    expect(orderStore.orders).toEqual([]);
    expect(orderStore.currentOrder).toBeNull();
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test loading all orders
   */
  it('should load all orders successfully', async () => {
    // Arrange
    vi.spyOn(apiService, 'fetchOrders').mockResolvedValue(mockOrders);
    
    // Act
    await orderStore.loadOrders();
    
    // Assert
    expect(apiService.fetchOrders).toHaveBeenCalledTimes(1);
    expect(orderStore.orders).toEqual(mockOrders);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test error handling when loading orders fails
   */
  it('should handle errors when loading orders fails', async () => {
    // Arrange
    vi.spyOn(apiService, 'fetchOrders').mockRejectedValue(new Error('Network error'));
    
    // Act
    await orderStore.loadOrders();
    
    // Assert
    expect(orderStore.orders).toEqual([]);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBe('Failed to fetch orders. Please try again later.');
  });

  /**
   * Test loading a single order by ID
   */
  it('should load a single order by ID', async () => {
    // Arrange
    vi.spyOn(apiService, 'fetchOrder').mockResolvedValue(mockSingleOrder);
    
    // Act
    await orderStore.loadOrder(3);
    
    // Assert
    expect(apiService.fetchOrder).toHaveBeenCalledWith(3);
    expect(orderStore.currentOrder).toEqual(mockSingleOrder);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test error handling when loading a single order fails
   */
  it('should handle errors when loading a single order fails', async () => {
    // Arrange
    vi.spyOn(apiService, 'fetchOrder').mockRejectedValue(new Error('Network error'));
    
    // Act
    await orderStore.loadOrder(3);
    
    // Assert
    expect(orderStore.currentOrder).toBeNull();
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBe('Failed to fetch order with ID 3. Please try again later.');
  });

  /**
   * Test creating a new order
   */
  it('should create a new order successfully', async () => {
    // Arrange
    vi.spyOn(apiService, 'createOrder').mockResolvedValue(mockSingleOrder);
    const newOrderData = {
      customerId: '13579',
      items: [{ productId: 103, quantity: 3 }]
    };
    
    // Act
    const result = await orderStore.createNewOrder(newOrderData);
    
    // Assert
    expect(apiService.createOrder).toHaveBeenCalledWith(newOrderData);
    expect(result).toEqual(mockSingleOrder);
    expect(orderStore.orders).toContainEqual(mockSingleOrder);
    expect(orderStore.currentOrder).toEqual(mockSingleOrder);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test error handling when creating an order fails
   */
  it('should handle errors when creating an order fails', async () => {
    // Arrange
    vi.spyOn(apiService, 'createOrder').mockRejectedValue(new Error('Network error'));
    const newOrderData = {
      customerId: '13579',
      items: [{ productId: 103, quantity: 3 }]
    };
    
    // Act
    const result = await orderStore.createNewOrder(newOrderData);
    
    // Assert
    expect(result).toBeNull();
    expect(orderStore.orders).toEqual([]);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBe('Failed to create order. Please try again later.');
  });

  /**
   * Test updating an existing order
   */
  it('should update an existing order successfully', async () => {
    // Arrange
    const updatedOrder = {
      ...mockSingleOrder,
      status: 'updated'
    };
    vi.spyOn(apiService, 'updateOrder').mockResolvedValue(updatedOrder);
    
    // First load orders to populate the orders array
    vi.spyOn(apiService, 'fetchOrders').mockResolvedValue(mockOrders);
    await orderStore.loadOrders();
    
    // Act
    const result = await orderStore.updateExistingOrder(3, mockSingleOrder);
    
    // Assert
    expect(apiService.updateOrder).toHaveBeenCalledWith(3, mockSingleOrder);
    expect(result).toEqual(updatedOrder);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test error handling when updating an order fails
   */
  it('should handle errors when updating an order fails', async () => {
    // Arrange
    vi.spyOn(apiService, 'updateOrder').mockRejectedValue(new Error('Network error'));
    
    // Act
    const result = await orderStore.updateExistingOrder(3, mockSingleOrder);
    
    // Assert
    expect(result).toBeNull();
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBe('Failed to update order with ID 3. Please try again later.');
  });

  /**
   * Test deleting an order
   */
  it('should delete an order successfully', async () => {
    // Arrange
    vi.spyOn(apiService, 'deleteOrder').mockResolvedValue(undefined);
    vi.spyOn(apiService, 'fetchOrders').mockResolvedValue(mockOrders);
    
    // First load orders to populate the orders array
    await orderStore.loadOrders();
    orderStore.currentOrder = mockOrders[0];
    
    // Act
    const result = await orderStore.deleteExistingOrder(1);
    
    // Assert
    expect(apiService.deleteOrder).toHaveBeenCalledWith(1);
    expect(result).toBe(true);
    expect(orderStore.orders).not.toContainEqual(mockOrders[0]);
    expect(orderStore.currentOrder).toBeNull();
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test error handling when deleting an order fails
   */
  it('should handle errors when deleting an order fails', async () => {
    // Arrange
    vi.spyOn(apiService, 'deleteOrder').mockRejectedValue(new Error('Network error'));
    
    // Act
    const result = await orderStore.deleteExistingOrder(1);
    
    // Assert
    expect(result).toBe(false);
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBe('Failed to delete order with ID 1. Please try again later.');
  });

  /**
   * Test the reset method
   */
  it('should reset the store to its initial state', async () => {
    // Arrange
    vi.spyOn(apiService, 'fetchOrders').mockResolvedValue(mockOrders);
    vi.spyOn(apiService, 'fetchOrder').mockResolvedValue(mockSingleOrder);
    
    // Populate the store
    await orderStore.loadOrders();
    await orderStore.loadOrder(3);
    orderStore.setError('Some error');
    
    // Verify store has data
    expect(orderStore.orders).toHaveLength(2);
    expect(orderStore.currentOrder).not.toBeNull();
    expect(orderStore.error).not.toBeNull();
    
    // Act
    orderStore.reset();
    
    // Assert
    expect(orderStore.orders).toEqual([]);
    expect(orderStore.currentOrder).toBeNull();
    expect(orderStore.loading).toBe(false);
    expect(orderStore.error).toBeNull();
  });

  /**
   * Test helper method for setting loading state
   */
  it('should set loading state correctly', () => {
    // Arrange
    expect(orderStore.loading).toBe(false);
    
    // Act
    orderStore.setLoading(true);
    
    // Assert
    expect(orderStore.loading).toBe(true);
    
    // Act again
    orderStore.setLoading(false);
    
    // Assert
    expect(orderStore.loading).toBe(false);
  });

  /**
   * Test helper method for setting error state
   */
  it('should set error state correctly', () => {
    // Arrange
    expect(orderStore.error).toBeNull();
    
    // Act
    orderStore.setError('Some error occurred');
    
    // Assert
    expect(orderStore.error).toBe('Some error occurred');
    
    // Act again
    orderStore.setError(null);
    
    // Assert
    expect(orderStore.error).toBeNull();
  });
}); 