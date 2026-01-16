import { Database } from '../../types/database';

export const buildDatabase = ({
  currentMember,
  items = [],
  itemMemberships = [],
  members,
}: Partial<Database> = {}): Database => ({
  currentMember,
  items,
  itemMemberships,
  members: members ?? (currentMember ? [currentMember] : []),
});