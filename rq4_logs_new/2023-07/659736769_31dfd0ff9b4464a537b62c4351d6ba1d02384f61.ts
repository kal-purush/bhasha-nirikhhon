import { Replace } from 'src/helpers/Replace';
import { CategoryProps } from './category.entity';
import { ExpenseProps } from './expense.entity';
import { IncomeProps } from './income.entity';
import { ObjectiveProps } from './objectives.entity';
import { PlanningProps } from './planning.entity';
import { WalletProps } from './wallet.entity';
import { randomUUID } from 'crypto';

export interface UserProps {
  createdAt: Date;
  updatedAt: Date;
  name: string;
  email: string;
  password: string;
  avatar?: string;
  wallet?: WalletProps;
  categories?: CategoryProps[];
  expenses?: ExpenseProps[];
  incomes?: IncomeProps[];
  objetives?: ObjectiveProps[];
  plannings?: PlanningProps[];
}

export class User {
  private _id: string;
  private props: UserProps;

  constructor(
    props: Replace<UserProps, { createdAt?: Date; updatedAt?: Date }>,
    id?: string,
  ) {
    this._id = id ?? randomUUID();
    this.props = {
      ...props,
      createdAt: props.createdAt ?? new Date(),
      updatedAt: props.createdAt ?? new Date(),
    };
  }

  public get id(): string {
    return this._id;
  }

  public set name(name: string) {
    this.props.name = name;
  }

  public get name(): string {
    return this.props.name;
  }

  public set email(email: string) {
    this.props.email = email;
  }

  public get email(): string {
    return this.props.email;
  }

  public set password(password: string) {
    this.props.password = password;
  }

  public get password(): string {
    return this.props.password;
  }

  public set avatar(avatar: string) {
    this.props.avatar = avatar;
  }

  public get avatar(): string {
    return this.props.avatar;
  }

  public set wallet(wallet: WalletProps) {
    this.props.wallet = wallet;
  }

  public get wallet(): WalletProps {
    return this.props.wallet;
  }

  public set categories(category: CategoryProps) {
    this.props.categories.push(category);
  }

  public get categories(): CategoryProps[] {
    return this.props.categories;
  }

  public set expenses(expense: ExpenseProps) {
    this.props.expenses.push(expense);
  }

  public get expenses(): ExpenseProps[] {
    return this.props.expenses;
  }

  public set incomes(income: IncomeProps) {
    this.props.incomes.push(income);
  }

  public get incomes(): IncomeProps[] {
    return this.props.incomes;
  }

  public set objetives(objetive: ObjectiveProps) {
    this.props.objetives.push(objetive);
  }

  public get objetives(): ObjectiveProps[] {
    return this.props.objetives;
  }

  public set plannings(planning: PlanningProps) {
    this.props.plannings.push(planning);
  }

  public get plannings(): PlanningProps[] {
    return this.props.plannings;
  }

  public get createdAt(): Date {
    return this.props.createdAt;
  }

  public set updatedAt(date: Date) {
    this.props.updatedAt = date;
  }

  public get updatedAt(): Date {
    return this.props.createdAt;
  }
}