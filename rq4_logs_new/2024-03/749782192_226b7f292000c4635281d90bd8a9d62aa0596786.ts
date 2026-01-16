import { MemberStatus, Prisma, Role } from '@prisma/client';
import { TransactionClient } from './common.interface';
import { hashMembersPassword } from './utils';

const defaultAdmins: Prisma.MemberCreateInput[] = [
  {
    email: 'admin@test.com',
    password: 'admin',
    nickname: 'admin',
    status: MemberStatus.ACTIVE,
    role: Role.ADMIN,
  },
];

const defaultUsers: Prisma.MemberCreateInput[] = [
  {
    email: 'user1@test.com',
    password: 'user1',
    nickname: 'user1',
    status: MemberStatus.ACTIVE,
    role: Role.USER,
  },
  {
    email: 'user2@test.com',
    password: 'user2',
    nickname: 'user2',
    status: MemberStatus.INACTIVE,
    role: Role.USER,
  },
  {
    email: 'user3@test.com',
    password: 'user3',
    nickname: 'user3',
    status: MemberStatus.DELETED,
    role: Role.USER,
  },
];

const defaultGuides: Prisma.MemberCreateInput[] = [
  {
    email: 'guide1@test.com',
    password: 'guide1',
    nickname: 'guide1',
    status: MemberStatus.ACTIVE,
    role: Role.GUIDE,
  },
  {
    email: 'guide2@test.com',
    password: 'guide2',
    nickname: 'guide2',
    status: MemberStatus.INACTIVE,
    role: Role.GUIDE,
  },
  {
    email: 'guide3@test.com',
    password: 'guide3',
    nickname: 'guide3',
    status: MemberStatus.DELETED,
    role: Role.GUIDE,
  },
];

export async function defaultMemberSeed(client: TransactionClient) {
  const members = [...defaultAdmins, ...defaultUsers, ...defaultGuides];

  const memberData = await hashMembersPassword(members);

  await Promise.all(
    memberData.map((member) => {
      return client.member.create({
        data: member,
      });
    }),
  );
}