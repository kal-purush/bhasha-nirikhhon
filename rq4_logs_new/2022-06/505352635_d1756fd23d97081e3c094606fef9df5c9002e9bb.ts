import { CustomRepository } from "src/configs/typeorm-ex/typeorm-ex.decorator";
import { Repository } from "typeorm";
import { Book } from "./book.entity";
import { CreateBookDto } from "./dto/create-book.dto";

@CustomRepository(Book)
export class BookRepository extends Repository<Book> {

  async createBook(createBookDto: CreateBookDto): Promise<Book> {
    const { title, author, category, publisher } = createBookDto;

    const book = this.create({
      title,
      author,
      category,
      publisher
    });

    await this.save(book);
    return book;
  }
}