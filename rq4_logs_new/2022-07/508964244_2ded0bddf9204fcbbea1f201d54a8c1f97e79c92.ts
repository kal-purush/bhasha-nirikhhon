import UUID from "$utils/uuid";
import {randomArr} from "$utils/random";
import {NextApiRequest, NextApiResponse} from "next";

const users: Users[] = [...Array(24)].map((_, index) => ({
    id: new UUID().toString(),
    avatarUrl: `/static/avatar/avatar_${index + 1}.jpg`,
    name: randomArr([
        'Tom',
        'Jerry',
        'Caliban',
        'Butch Cat',
        'Preeta',
        'Linda',
        'Joy',
        'Estate',
        'Mr.Methta',
        'DuBros',
    ]),
    country: randomArr([
        'Belarus',
        'Anguilla',
        'Finland',
        'Egypt',
        'Congo',
        'Germany',
        'Iceland',
        'Italy',
        'Jersey',
        'Luxembourg',
    ]),
    isVerified: randomArr([
        'false',
        'true',
    ]),
    status: randomArr(['active', 'banned']),
    role: randomArr([
        'Leader',
        'Hr Manager',
        'UI Designer',
        'UX Designer',
        'UI/UX Designer',
        'Project Manager',
        'Backend Developer',
        'Full Stack Designer',
        'Front End Developer',
        'Full Stack Developer',
    ]),
}));

export default function UserReq(
    req: NextApiRequest,
    res: NextApiResponse
) {
    res.status(200).json({
        data: users,
        code: 0,
        msg: ''
    })
}

export interface Users {
    id: string
    avatarUrl: string
    name: string
    country: string
    isVerified: string
    status: string
    role: string
}