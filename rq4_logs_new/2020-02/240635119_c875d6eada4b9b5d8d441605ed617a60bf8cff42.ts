import {BookAccess} from '../data-layer/book-access'
import {Book} from '../models/book'
import { CreateBookRequest } from '../requests/CreateBookRequest';
import { getUserId } from '../lambda/utils';
import * as  uuid from 'uuid'
import { APIGatewayProxyEvent } from 'aws-lambda'
import { UpdateBookRequest } from '../requests/UpdateBookRequest'

const bookAccess = new BookAccess();
const bucketName = process.env.IMAGES_S3_BUCKET


export async function getAllBooks(): Promise<Book[]>{
  return bookAccess.getAllBooks()
}

// export async function getAllTodos(userId: string): Promise<Todo[]>{
//   return bookAccess.getAllTodos(userId)
// }

export async function createBook(event: APIGatewayProxyEvent): Promise<Book>{
  
  const userIdHead = getUserId(event);
  const parsedBody: CreateBookRequest = JSON.parse(event.body)
  const itemId: string = uuid.v4()
  const date = new Date();
  const dateString = date.toISOString();
  const newItem = {
    id: itemId,
    userId: userIdHead,
    createdAt: dateString,
    attachmentUrl: `https://${bucketName}.s3.amazonaws.com/${itemId}`,
    ...parsedBody
  }

  const result = await bookAccess.createBook(newItem);

  return result;
}

export async function updateBook(bookId: string, updatedBook: UpdateBookRequest): Promise<string>{
  return bookAccess.updateBook(bookId, updatedBook)
}

export async function deleteBook(bookId: string): Promise<string>{
  return bookAccess.deleteBook(bookId)
}