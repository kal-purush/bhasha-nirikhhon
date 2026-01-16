declare class Utils {
    constructor();
    static swap(a: any, b: any): any[];
    static calculate(func: any, arr: number[]): number;
    static generateHTML(arr: any[], type: string): HTMLUListElement;
    static siftDown(numbers: number[], root: number, bottom: number): void;
    static merge(numbers: number[], begin: number, mid: number, end: number): number[];
    /** Partition method */
    static partition(numbers: number[], left: number, right: number): number;
}
/**
 * Algorithms
 */
declare class Sort {
    constructor();
    /**
     * Uneficient bubbleSort
     */
    bubbleSort1(numbers: number[]): void;
    /**
     * Most eficient bubbleSort
     */
    bubbleSort2(numbers: number[]): void;
    /**
     * Quick sort
     */
    quickSort(numbers: number[], left: number, right: number): number[];
    /**
     * Insertion Sort
     */
    insertionSort(numbers: number[]): void;
    /**
     * Shell Sort
     */
    shellSort(numbers: number[]): void;
    /**
     * Selection Sort
     */
    selectionSort(numbers: number[]): void;
    /**
     * Heap Sort
     */
    heapSort(numbers: number[]): void;
    /**
     * Merge Sort
     */
    mergeSort(numbers: number[], begin: number, end: number): number[];
}