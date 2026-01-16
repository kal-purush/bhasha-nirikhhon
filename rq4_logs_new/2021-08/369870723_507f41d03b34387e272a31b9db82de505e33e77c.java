package com.curvelo;

import com.curvelo.database.model.BookModel;
import com.curvelo.database.model.BookModel.BookModelBuilder;
import com.curvelo.database.model.ReviewModel;
import com.curvelo.database.model.ReviewModel.ReviewModelBuilder;
import com.curvelo.database.model.UserModel;
import com.curvelo.database.model.UserModel.UserModelBuilder;

public class ComposeModel {

  private ComposeModel() {}

  public static BookModelBuilder createBook(final int bookId) {
    return BookModel.builder()
        .id(bookId)
        .isbn("123456789")
        .numberOfPages(250)
        .author("J.R.R. Tolkien")
        .title("Hobbit");
  }

  public static UserModelBuilder createUser(final int userId) {
    return UserModel.builder()
        .id(userId)
        .name("Igor");
  }

  public static ReviewModelBuilder createReview(final int reviewId,
      final UserModel userModel, final BookModel bookModel) {
    return ReviewModel.builder()
        .id(reviewId)
        .comment("excelente leitura")
        .score(4)
        .book(bookModel)
        .user(userModel);
  }

}