import { CastingDto } from '../dto/casting.dto'
import { Casting } from '../casting.entity'

const mockDate = new Date()

export const mockCastingDto: CastingDto = {
    companyId: 1,
    title: 'title',
    description: 'description',
    city: 'city',
    lat: 1,
    lng: 1,
    startDate: '11-07-2019',
    duration: '12h'
}

export const mockCasting: Casting = {
    ...mockCastingDto,
    id: 1,
    createdAt: mockDate,
    updatedAt: mockDate,
    company: {
        id: 1,
        name: 'name',
        logo: 'logo',
        description: 'desc',
        userId: 1,
        castings: []
    }
}

export const mockCastings: Casting[] = [mockCasting, mockCasting]