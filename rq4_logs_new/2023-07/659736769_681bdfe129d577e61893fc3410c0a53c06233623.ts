import { CategoryProps } from '@application/entities/category.entity';

export const DefaultCategories: CategoryProps[] = [
  {
    name: 'Salário',
    type: 'income',
    createdAt: new Date(),
  },
  {
    name: 'Investimento',
    type: 'income',
    createdAt: new Date(),
  },
  {
    name: 'Presente',
    type: 'income',
    createdAt: new Date(),
  },
  {
    name: 'Outros',
    type: 'income',
    createdAt: new Date(),
  },
  {
    name: 'Casa',
    type: 'expense',
    createdAt: new Date(),
  },
  {
    name: 'Lazer',
    type: 'expense',
    createdAt: new Date(),
  },
  {
    name: 'Serviços',
    type: 'expense',
    createdAt: new Date(),
  },
  {
    name: 'Outros',
    type: 'expense',
    createdAt: new Date(),
  },
];