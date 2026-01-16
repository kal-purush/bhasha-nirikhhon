import { schema } from "@blank/zero";
import {
  CustomMutatorDefs,
  Transaction as TransactionInternal,
} from "@rocicorp/zero";
import { slug } from "./utils";
import { OpenAuthToken } from "@blank/auth/subjects";

const CONSTRAINTS = {
  GROUPS: {
    MAX_USER_CAN_OWN: 4,
    MAX_USER_CAN_BE_MEMBER_OF: 8,
  },
};

export type ClientMutators = ReturnType<typeof createClientMutators>;
export type CreateMutators = CustomMutatorDefs<typeof schema>;
type Transaction = TransactionInternal<typeof schema>;

export type CreateGroupOptions = {
  userId: string;
  username: string;
  title: string;
  description: string;
};

export type DeleteGroupOptions = { groupId: string };

const queries = {
  users: (tx: Transaction, userId: string) => {
    return {
      findAllMemberships: () =>
        tx.query.group
          .whereExists("members", (member) => member.where("userId", userId))
          .run(),
    };
  },
  groups: (tx: Transaction, groupId: string) => {
    return {
      findAllMembers: () => tx.query.member.where("groupId", groupId).run(),
      findAllExpenses: () => tx.query.expense.where("groupId", groupId).run(),
      findAllUserPreferences: () =>
        tx.query.preference.where("defaultGroupId", groupId).run(),
    };
  },
};

const assertUserCanCreateAndJoinGroup = async (
  tx: Transaction,
  userId: string
) => {
  const numGroupsUserOwns = (
    await queries.users(tx, userId).findAllMemberships()
  ).length;

  if (numGroupsUserOwns >= CONSTRAINTS.GROUPS.MAX_USER_CAN_OWN) {
    throw new Error(
      `User can not own more than ${CONSTRAINTS.GROUPS.MAX_USER_CAN_OWN.toString()} groups`
    );
  }

  if (numGroupsUserOwns >= CONSTRAINTS.GROUPS.MAX_USER_CAN_OWN) {
    throw new Error(
      `User can not be a member of more than ${CONSTRAINTS.GROUPS.MAX_USER_CAN_OWN.toString()} groups`
    );
  }
};

export function createClientMutators(_auth: OpenAuthToken | undefined) {
  return {
    group: {
      create: async (tx, opts: CreateGroupOptions) => {
        await assertUserCanCreateAndJoinGroup(tx, opts.userId);
        const location = import.meta.env.SSR ? "server" : "client";
        console.log(`running on ${location}: `, opts.userId);

        const groupId = crypto.randomUUID();

        await tx.mutate.group.insert({
          id: groupId,
          title: opts.title,
          slug: slug(opts.title).encode(),
          description: opts.description,
          ownerId: opts.userId,
          createdAt: Date.now(),
        });

        // NOTE: we can call a helper and/or member function here to be re-usable
        await tx.mutate.member.insert({
          groupId,
          userId: opts.userId,
          nickname: opts.username,
        });
      },
      delete: async (tx, opts: DeleteGroupOptions) => {
        const q = queries.groups(tx, opts.groupId);

        await tx.mutate.group.delete({ id: opts.groupId });

        for (const { groupId, userId } of await q.findAllMembers()) {
          await tx.mutate.member.delete({ groupId, userId });
        }

        for (const { id } of await q.findAllExpenses()) {
          await tx.mutate.expense.delete({ id });
        }
      },
    },
  } as const satisfies CreateMutators;
}