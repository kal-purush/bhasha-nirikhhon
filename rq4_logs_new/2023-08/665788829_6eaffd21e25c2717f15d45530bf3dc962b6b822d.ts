export type Category = "fixed" | "scheduled";



export type Advicedata = {
  id: number;
  title: string;
  contents: string;
  date: Date;
};

export type ClientData = {
  id: number;
  name: string;
  phone: string;
  email: string;
  address: string;
};

export type ExpenseData = {
  id: number;
  lawsuitId: number;
  speningAt: Date;
  contents: string;
  amount: number;
  isDeleted: boolean;
};

export type LawsuitType = "형사" | "민사";
export type LawsuitStatus = "등록" | "진행" | "종결";
export type LawsuitData = {
  id: number;
  name: string;
  lawsuitNum: string;
  lawsuitType: LawsuitType;
  lawsuitStatus: LawsuitStatus;
  court: string;
  commissionFee: number;
  contingentFee: number;
};

export type MemberData = {
  id: number;
  email: string;
  password: string;
  name: string;
  phone: string;
  address: string;
  hierarchyId: number;
  roleId: number;
};

export type ReceptionData = {
  id: number;
  lawsuitId: number;
  deadline: Date;
  status: boolean;
  contents: string;
  receivedAt: Date;
  category: Category;
  isDeleted: boolean;
};