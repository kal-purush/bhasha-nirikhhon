import { Body, Get, HttpCode, HttpError, JsonController, Param, Patch, Post } from 'routing-controllers';
import { Book } from '../entity/Book';
import { Rental } from '../entity/Rental';
import { User } from '../entity/User';
import { BaseController } from './BaseController';

@JsonController()
export class RentalController extends BaseController {
  // 특정 사용자가 가지고 있는 모든 대여 목록 가져옴
  @Get('/rentals/users/:userId([0-9]+)')
  public async findUserBooks(@Param('userId') userId: number) {
    const rentals: Rental[] = await Rental.find({ where: { userId: userId }, relations: ['book'] });
    if (rentals.length === 0) throw new HttpError(204);
    return rentals;
  }

  // 특정 사용자가 가지고 있는 특정 책의 대여 정보 가져옴
  @Get('/rentals/users/:userId([0-9]+)/books/:bookId([0-9]+)')
  public async findUserBook(@Param('userId') userId: number, @Param('bookId') bookId: number) {
    const rental: Rental | undefined = await Rental.findOne({ where: { userId: userId, bookId: bookId }, relations: ['book'] });
    if (!rental) throw new HttpError(404, '해당 유저에 대한 책 대여 정보를 찾을 수 없습니다.');
    return rental;
  }

  // 특정 사용자가 책을 대여함
  @HttpCode(201)
  @Post('/rentals')
  public async RentalBook(@Body({ validate: true }) rental: Rental) {
    const user: User | undefined = await User.findOne(rental.userId);
    if (!user) throw new HttpError(404, '해당 유저를 찾을 수 없습니다.');

    const book: Book | undefined = await Book.findOne(rental.bookId);
    if (!book) throw new HttpError(404, '해당 책을 찾을 수 없습니다.');

    const rentalInfo: Rental | undefined = await Rental.findOne({ where: { userId: rental.userId, bookId: rental.bookId } });
    // 한 사람이 책을 대여 하는 중이면 생성 불가해야하고
    // 반납을 완료한 상태에서는 다시 대여가 가능해야하는지?
    if (rentalInfo) throw new HttpError(422, '이미 대여 기록이 존재합니다.');

    return rental.save();
  }

  // 특정 사용자가 책을 반납함
  @Patch('/rentals/:id([0-9]+)')
  public async returnBook(@Param('id') rentalId: number) {
    const rental: Rental | undefined = await Rental.findOne({ where: { id: rentalId } });
    if (!rental) throw new HttpError(404, '존재하지 않는 대여 기록입니다.');
    if (rental.returnDateAt) throw new HttpError(422, '이미 반납한 책입니다.');

    rental.returnDateAt = new Date();

    return rental.save();
  }
}