import { Author, Book } from "@prisma/client";

export type IBook = Book & { authors: Author[] };