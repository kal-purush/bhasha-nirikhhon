import {FindAllTeamMemberQueryDto, TeamMemberDto} from '^types/team-member.type';
import {atom} from 'recoil';
import {Paginated} from '^types/utils/paginated.dto';

export const teamMemberSearchParams = atom<FindAllTeamMemberQueryDto>({
    key: 'teamMemberSearchParams',
    default: {},
});

export const teamMemberSearchResults = atom<Paginated<TeamMemberDto>>({
    key: 'teamMemberSearchResults',
    default: {
        items: [],
        pagination: {
            totalItemCount: 0,
            currentItemCount: 0,
            totalPage: 1,
            currentPage: 1,
            itemsPerPage: 30,
        },
    },
});