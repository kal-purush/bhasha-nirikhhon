export interface IUser {
  userName: string;
  accountBalance: string;

  profile: {
    firstName: string;
    lastName: string;
    phoneNumber: string;

    avatar: string;
    gender: string;
    bvn: string;
    accountNumber: string;
    currency: string;
    email: string;
  };
  education: {
    officeEmail: string;
    level: string;
    employmentStatus: string;
    monthlyIncome: string;
    sector: string;
    loanRepayment: string;
    duration: string;
  };
  socials: {
    twitter: string;
    facebook: string;
    instagram: string;
  };
  guarantor: {
    firstName: string;
    lastName: string;
    phoneNumber: string;
    address: string;
    gender: string;
  };
}