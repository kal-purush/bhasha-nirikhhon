interface IRequest {
  sum: number;
  from: number;
  to: number;
}

type Status = 'success' | 'failed';

interface IStatus<S extends Status> {
  status: S;
}

interface ISuccessData {
  data: {
    databaseId: number;
    sum: number;
    from: number;
    to: number;
  };
}

interface IErrorData {
  data: {
    errorMessage: string;
    errorCode: number;
  };
}

interface ISuccessResponse extends ISuccessData, IStatus<'success'> {
}

interface IErrorResponse extends IErrorData, IStatus<'failed'> {
}

type InputValidate = IErrorResponse | ISuccessResponse;

// CHECK:

interface ICheckForValidStatus extends IErrorData, IStatus<'unsuccess'> { // error
}

const response1: InputValidate = { // ok
  status: 'success',
  data: {
    databaseId: 567,
    sum: 10000,
    from: 2,
    to: 4,
  },
};

const response2: InputValidate = { // ok
  status: 'failed',
  data: {
    errorMessage: 'Недостаточно средств',
    errorCode: 4,
  },
};

const response3: InputValidate = {
  status: 'failed',
  data: {
    databaseId: 567, // error
    sum: 10000,
    from: 2,
    to: 4,
  },
};

const response4: InputValidate = {
  status: 'success',
  data: {
    errorMessage: 'Недостаточно средств', // error
    errorCode: 4,
  },
};

const request1: IRequest = { // ok
  sum: 1000,
  from: 2,
  to: 4,
};

const request2: IRequest = {
  sum: '1000', // error
  from: 2,
  to: 4,
};

// Уявімо, що ми розробляємо платіжну систему.
// Бекенд розробники віддали нам ендпоінт, на який
// потрібно відправити наступну інформацію:

// {
// 	"sum": 10000,
// 	"from": 2,
// 	"to": 4
// }

// У відповідь сервер надішле один з наступних варіантів (при успіху або провалі)

// In case of success
// {
// 	"status": "success",
// 	"data": {
// 		"databaseId": 567,
// 		"sum": 10000,
// 		"from": 2,
// 		"to": 4
// 	}
// },

// In case of fail
// {
// 	"status": "failed",
// 	"data": {
// 		"errorMessage": "Недостаточно средств",
// 		"errorCode": 4
// 	}
// }

// Нам потрібно створити інтерфейси для об'єктів реквесту та обох варіантів респонсу.
// Будьте уважні, старайтеся не допускати дублювання та використовуйте знання,
// набуті на попередніх лекціях.
//
// P.S: Не треба робити реалізацію запиту на бекенд, створювати додаткову логіку і тд.
// В рамках цього завдання ми вчимося працювати з абстракціями