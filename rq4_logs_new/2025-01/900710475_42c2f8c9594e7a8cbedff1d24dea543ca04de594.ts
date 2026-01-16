import { Meteor } from 'meteor/meteor';
import Book from '../types/book';

// Function to handle the 'swipe left' action
export async function swipeLeft(): Promise<Book[]> {
  // Call the Meteor method 'server_swipLeft' asynchronously and fetch the books
  const books = await Meteor.callAsync('server_swipeLeft');
  return books;
}

// Function to handle the 'swipe right' action
export async function swipeRight(book_id: number): Promise<Book> {
  // Call the Meteor method 'server_swipRight' asynchronously with the book ID
  const books = await Meteor.callAsync('server_swipeRight', book_id);
  return books[0];
}