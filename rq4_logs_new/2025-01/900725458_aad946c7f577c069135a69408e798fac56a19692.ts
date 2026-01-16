class Database<T> {
  protected elements = new Array<T>();

  public insert(element: T) {
    this.elements.push(element);
  }

  public findOne(index: number): T | undefined {
    return this.elements[index];
  }

  public find() {
    this.elements.forEach((element) => console.log(element));
  }
}

const database = new Database<string>();
database.insert('홍길동');
const element = database.findOne(0);

class SpecialDatabase<T extends { id: string }> extends Database<T> {
  public delete(id: string) {
    const index = this.elements.findIndex((element) => element.id == id);

    this.elements.splice(index, 1);
  }
}