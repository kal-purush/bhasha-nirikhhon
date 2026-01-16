import { UseCase, MembersStore } from '../../types';

const deleteOneMember = ({
    membersStore,
}: {
    membersStore: MembersStore;
}): UseCase<boolean> => {
    return async function useCase({ id }) {

        const memberExists = await membersStore.memberExistsByFilter({
            _id: id,
        });

        if (!memberExists) {
            throw new Error(`Member doesn't exist`);
        }

        await membersStore.deleteOneMember({ _id: id });

        return true;
    };
};

export default deleteOneMember;