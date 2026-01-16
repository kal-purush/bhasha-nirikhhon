import { User } from "@/types/user";
import Papa from 'papaparse';

export const exportUser = async (users: User[]) => {
  const csv = Papa.unparse(users);
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = 'user.csv';
  link.click();
};