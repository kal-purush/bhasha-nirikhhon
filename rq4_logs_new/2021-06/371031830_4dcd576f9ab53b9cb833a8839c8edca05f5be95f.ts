interface Company {
  id: number;
  name: string;
  city: string;
  zipCode: string;
  createdAt: string;
  updatedAt: string;
}

interface User {
  id?: string;
  firstName: string;
  lastName: string;
  role?: 'USER' | 'ADMIN' | 'SUPERADMIN';
  email: string;
  password?: string;
  weeklyBasis?: 'h35' | 'h39';
  jobId: string;
  companyId?: string;
}

interface Job {
  id: string;
  label: string;
}

interface Project {
  id: number;
  name: string;
  description: string;
  code: string;
  taxation: string;
}

interface IRecord {
  id: string;
  userId: string;
  projectId: string;
  companyId: string;
  date: string;
  timeslot: 'MORNING' | 'AFTERNOON';
  comment: string;
}

interface SelectItem {
  value: string;
  text: string;
}