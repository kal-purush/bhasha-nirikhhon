import { UseCase, MembersStore } from '../../types';

const updateMember = ({
  memberEntity,
  membersStore,
}: {
  membersStore: MembersStore;
  memberEntity;
}): UseCase => {
  return async function ({ id, info }) {
    const memberExists = await membersStore.memberExistsByFilter({
      _id: id,
    });

    if (!memberExists) {
      throw new Error(`Member ID doesn't exist`);
    }

    const member = await memberEntity(info);

    const usernameExists = await membersStore.selectOneMemberByFilters({
      username: member.username,
      _id: { $ne: id },
    });

    if (usernameExists) {
      throw new Error(`Username already exists`);
    }

    await membersStore.updateMemberByFilters({ _id: id }, member);

    return true;
  };
};

export default updateMember;