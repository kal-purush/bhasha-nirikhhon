import { INavEmployee } from '@app/types/bruker';

export const formatEmployeeName = (employee: INavEmployee | null, fallback: string) =>
  employee === null ? fallback : `${employee.navn} (${employee.navIdent})`;