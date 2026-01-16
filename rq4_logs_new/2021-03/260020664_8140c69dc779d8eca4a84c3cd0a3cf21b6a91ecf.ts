import { CardModel } from '../../models/card-model';
import { Card } from './card';

fdescribe('Card', () => {
  let card: Card;

  beforeEach(() => {
    const mockCard: CardModel = {
      herbName: 'Bay',
      graphicsUrl: 'bay.png',
      isSpecial: false,
      points: 0,
    };
    card = new Card(mockCard);
  });

  it('should return valid path for images', () => {
    // Arrange
    const language = "pl";

    // Act
    const result =  card.getImagePath(language);

    // Assert
    const expectedResult = '../../../../assets/images/cards/faces/pl/bay.png';
    expect(result).toBe(expectedResult);
  });
});