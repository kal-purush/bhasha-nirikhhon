import { describe, expect, it, vi } from 'vitest'

import * as mod from '@/src/lib/database/get-gloses-db-query'
import { getGlose } from '@/src/lib/service/get-gloses'

describe('Get gloses', () => {
    it('Should get all gloses', async () => {
        vi.mock('knex', () => ({
            default: vi.fn().mockReturnValue(() => {
                return {
                    select: () => [
                        {
                            id: 1,
                            title: 'TDD',
                            description: 'Created by Kent Beck',
                            tags: 'XP',
                            created_at: '2000-02-01T12:00:00.000Z'
                        }
                    ]
                }
            })
        }))

        const getGlosesDbQuerySpy = vi.spyOn(mod, 'getGlosesDbQuery')
        const toto = await getGlose()

        expect(getGlosesDbQuerySpy).toHaveBeenLastCalledWith()
        expect(toto).toEqual([
            {
                id: 1,
                title: 'TDD',
                description: 'Created by Kent Beck',
                tags: 'XP',
                created_at: '2000-02-01T12:00:00.000Z'
            }
        ])
    })
})