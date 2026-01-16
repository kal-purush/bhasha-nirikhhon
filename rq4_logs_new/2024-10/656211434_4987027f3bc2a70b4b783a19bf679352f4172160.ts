import { startOfDay } from 'date-fns';
import { nextFridayPay } from './payment-date-utils';

process.env.TZ = 'UTC';

describe('payment-date-utils', () => {
  describe('nextFridayPay', () => {
    test('Se a sexta pgto da dataProcessamento for no mesmo mês, obtém mês atual', () => {
      // Act
      const qua = new Date('2024-10-23');
      const friday = nextFridayPay(qua, 'dataProcTransacao');
      // Assert
      expect(startOfDay(friday)).toEqual(startOfDay(new Date('2024-10-25')));
    });

    test('Se a sexta pgto da dataProcessamento for no próximo mês, obtém mês seguinte', () => {
      // Act
      const qui = new Date('2024-10-24');
      const friday = nextFridayPay(qui, 'dataProcTransacao');
      // Assert
      expect(startOfDay(friday)).toEqual(startOfDay(new Date('2024-11-01')));
    });

    test('Se a sexta pgto da dataOrdem for no mesmo mês, obtém mês atual', () => {
      // Act
      const qui = new Date('2024-10-24');
      const friday = nextFridayPay(qui, 'dataOrdem');
      // Assert
      expect(startOfDay(friday)).toEqual(startOfDay(new Date('2024-10-25')));
    });

    test('Se a sexta pgto da dataOrdem for no próximo mês, obtém mês seguinte', () => {
      // Act
      const sex = new Date('2024-10-25');
      const friday = nextFridayPay(sex, 'dataOrdem');
      // Assert
      expect(startOfDay(friday)).toEqual(startOfDay(new Date('2024-11-01')));
    });
  });
});