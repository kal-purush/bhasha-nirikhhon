import handler from '@/src/pages/api/gloses'
import { describe, expect, it, vi } from 'vitest'
import * as mod2 from '@/src/lib/service/get-gloses'
import * as mod from '@/src/lib/service/create-glose'

describe('Glose api', () => {
    it('should get gloses when request method is GET', async () => {
        const getGlosesSpy = vi.spyOn(mod2, 'getGlose')

        const request = {
            method: 'GET'
        }
        const res = {
            status: vi.fn().mockReturnValue({
                json: vi.fn().mockReturnValue({
                    message: 'Voici vos gloses',
                    gloses: []
                })
            })
        }
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-expect-error
        // TODO Type for request
        await handler(request, res)
        expect(getGlosesSpy).toHaveBeenCalledOnce()
    })

    it('should create a glose when request method is POST', async () => {
        vi.useFakeTimers()
        const date = new Date(2000, 1, 1, 13)
        vi.setSystemTime(date)

        vi.mock('@/src/lib/database/get-gloses-db-query')
        const createGlosesSpy = vi.spyOn(mod, 'createGlose')

        const request = {
            method: 'POST',
            body: '{"title":"TDD","description":"Created by Kent Beck","tags":"XP"}'
        }
        const res = {
            status: vi.fn().mockReturnValue({
                json: vi.fn().mockReturnValue({
                    message: 'Voici vos gloses',
                    gloses: []
                })
            })
        }
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-expect-error
        // TODO Type for request
        await handler(request, res)
        expect(createGlosesSpy).toHaveBeenCalledWith({
            title: 'TDD',
            description: 'Created by Kent Beck',
            tags: 'XP',
            created_at: '2000-02-01T12:00:00.000Z'
        })
    })
})