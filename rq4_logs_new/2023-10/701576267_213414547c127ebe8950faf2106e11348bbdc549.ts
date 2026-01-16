import { renderHook } from "@testing-library/react";
import { useProducts } from "./useProducts";



const products = [
  { id: 3, title: "Product Ab" },
  { id: 1, title: "Product A" },
  { id: 2, title: "Product B" },
];

jest.mock("../useFilter/useFilter", () => ({
  useFilterStore: jest.fn().mockReturnValue({ filter: "product a", order: 0 }), // default order
}));

describe("useProducts", () => {
  it("filters and sorts products correctly", () => {
    
    const { result } = renderHook(() => useProducts(products as any));

    const filteredProducts = result.current;
  
    expect(filteredProducts).toEqual([
      { id: 1, title: "Product A" }, //filtered data sorted by id (default order)      
      { id: 3, title: "Product Ab" }
    ]);
  });
});