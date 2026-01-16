import { renderHook, act } from '@testing-library/react';
import { useConfirmationDialog } from './confirmation-dialog.hook';

describe('confirmation dialog hook specs', () => {

  it('should have the correct initial state', () => {
    const { result } = renderHook(() => useConfirmationDialog());
    expect(result.current.isOpen).toBe(false);
    expect(result.current.itemToDelete).toEqual({ id: '', name: '' });
  });


  it('should open the dialog when onOpenDialog is called', () => {
    const { result } = renderHook(() => useConfirmationDialog());
    const itemToDelete = { id: '1', name: 'Test Item' };
    act(() => {
      result.current.onOpenDialog(itemToDelete);
    });
    expect(result.current.isOpen).toBe(true);
    expect(result.current.itemToDelete).toEqual(itemToDelete);
  });


  it('should close the dialog when onClose is called', () => {
    const { result } = renderHook(() => useConfirmationDialog());
    act(() => {
      result.current.onOpenDialog({ id: '1', name: 'Test Item' });
    });
    expect(result.current.isOpen).toBe(true);
    act(() => {
      result.current.onClose();
    });
    expect(result.current.isOpen).toBe(false);
  });

  
  it('should clear the itemToDelete when onAccept is called', () => {
    const { result } = renderHook(() => useConfirmationDialog());
    act(() => {
      result.current.onOpenDialog({ id: '1', name: 'Test Item' });
    });
    expect(result.current.itemToDelete).not.toEqual({ id: '', name: '' });
    act(() => {
      result.current.onAccept();
    });
    expect(result.current.itemToDelete).toEqual({ id: '', name: '' });
  });
});