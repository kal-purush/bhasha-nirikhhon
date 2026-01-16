/**
 * @file Controller RESTful Web service API for bookmarks resource
*/

import BookmarkControllerI from "../interfaces/BookmarkControllerI";
import BookmarkDaoI from "../interfaces/BookmarkDaoI";
import { Request, Response, Express } from "express";

 /**
  * @class BookmarkController Implements RESTful Web service API for bookmarks resource.
  * Defines the following HTTP endpoints:
  * <ul>
  *     <li> POST /api/users/:uid/bookmarks/:tid to bookmark a tuit</li>
  *     <li> DELETE /api/users/:uid/bookmarks/:tid to delete a bookmark </li>
  *     <li> GET /api/users/:uid/bookmarks to get all bookmarked tuits by user </li>
  * </ul>
  * @property {BookmarkDaoI} bookmarkDao Singleton DAO implementing message CRUD operations
  * @property {BookmarkController} bookmarkController Singleton controller implementing
  * RESTful Web service API
  */
export default class BookmarkController implements BookmarkControllerI {

  private static bookmarkController: BookmarkControllerI | null = null;

  private static bookmarkDao: BookmarkDaoI;

  /**
  * Creates singleton controller instance
  * @param {Express} app Express instance to declare the RESTful Web service API
  * @return BookmarkController
  */
  public static getInstance = (app: Express, bookmarkDao: BookmarkDaoI): BookmarkController => {

    if (BookmarkController.bookmarkController === null) {
      BookmarkController.bookmarkController = new BookmarkController();
    }

    BookmarkController.bookmarkDao = bookmarkDao;
    
    app.post('/api/users/:uid/bookmarks/:tid', BookmarkController.bookmarkController.createBookmark);
    app.delete('/api/users/:uid/bookmarks/:tid', BookmarkController.bookmarkController.unBookmark);
    app.get('/api/users/:uid/bookmarks', BookmarkController.bookmarkController.getAllBookmarkedTuitsbyUser);

    return BookmarkController.bookmarkController;
  }

  private constructor() {}

  /**
  * Bookmarks new tuit
  * @param {Request} req Represents request from client, including the
  * path parameters uid and tid representing the user that is bookmarking and the tuit getting bookmarked
  * @param {Response} res Represents response to client, including the newly created bookmark object
  */
  createBookmark(req: Request, res: Response): void {
    BookmarkController.bookmarkDao
      .createBookmark(req.params.uid, req.params.tid)
      .then(bookmark => res.json(bookmark));
  }

  /**
  * Unbookmarks tuit
  * @param {Request} req Represents request from client, including the
  * path parameters uid and tid representing the user that is unbookmarking and the tuit getting unbookmarked
  * @param {Response} res Represents response to client, including the status of the response
  */
  unBookmark(req: Request, res: Response): void {
    BookmarkController.bookmarkDao
      .unBookmark(req.params.uid, req.params.tid)
      .then(status => res.json(status));
  }


  /**
  * Retrieves all tuits bookmarked by a user
  * @param {Request} req Represents request from client, including the
  * path parameters uid representing the user 
  * @param {Response} res Represents response to client, including the JSON array of tuits
  */
  getAllBookmarkedTuitsbyUser(req: Request, res: Response): void {
    BookmarkController.bookmarkDao
      .getAllBookmarkedTuitsbyUser(req.params.uid)
      .then(tuits => res.json(tuits));
  }
  
}