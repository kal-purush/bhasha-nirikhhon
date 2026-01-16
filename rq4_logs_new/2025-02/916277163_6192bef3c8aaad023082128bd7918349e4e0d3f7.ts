import React from 'react';

import { EGender, ERole, EStatus } from '@repo/business/shared/enum';
import { User } from '@repo/business/auth/interface';

export interface UserContextProps {
  user: User;
  update: (user: User) => void;
}

export const initialUserData: User = {
  id: '',
  cpf: '',
  name: '',
  role: ERole.USER,
  email: '',
  gender: EGender.OTHER,
  status: EStatus.INACTIVE,
  whatsapp: '',
  created_at: new Date(),
  updated_at: new Date(),
  date_of_birth: new Date(),
};

export default React.createContext<UserContextProps>({
  user: initialUserData,
  update: () => {},
});