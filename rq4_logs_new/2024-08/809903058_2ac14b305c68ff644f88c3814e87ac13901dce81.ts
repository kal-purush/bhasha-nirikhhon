export abstract class CustomSet<T> {
  private map: Map<string, T>;

  constructor() {
    this.map = new Map<string, T>();
  }

  // Hash function to generate a unique hash for each object
  abstract hash(value: T): string;

  add(value: T): void {
    const key = this.hash(value);
    if (!this.map.has(key)) {
      this.map.set(key, value);
    }
  }

  delete(value: T): boolean {
    const key = this.hash(value);
    return this.map.delete(key);
  }

  has(value: T): boolean {
    const key = this.hash(value);
    return this.map.has(key);
  }

  clear(): void {
    this.map.clear();
  }

  size(): number {
    return this.map.size;
  }

  values(): T[] {
    return Array.from(this.map.values());
  }
}

export class DateSet extends CustomSet<Date> {
  hash(value: Date): string {
    return value.toLocaleDateString();
  }
}