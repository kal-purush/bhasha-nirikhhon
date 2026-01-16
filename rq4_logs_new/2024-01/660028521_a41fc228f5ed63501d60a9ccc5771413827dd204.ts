import { createContext } from 'react';
import AllCompensationsState from './AllCompensationsState';

const AllCompensationsStateContext = createContext<AllCompensationsState>(null as unknown as AllCompensationsState);

export default AllCompensationsStateContext;