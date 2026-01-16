import { describe, expect, it, vi } from 'vitest'
import { addGlossTerm, getAllGlosesTerms } from '@/src/actions/gloses-actions'
import { InitialState } from '@/src/components/gloss-terms-form'
import { createFetchResponse, gloses } from '@/__tests__/helpers'

describe('Gloses actions', () => {
    describe('addGlossTerm', async () => {
        const formData = {
            get: () => 'some words',
            append: vi.fn(),
            delete: vi.fn(),
            getAll: vi.fn(),
            has: vi.fn(),
            set: vi.fn(),
            forEach: vi.fn(),
            entries: vi.fn(),
            keys: vi.fn(),
            values: vi.fn()
        }
        const initialState: InitialState = {
            message: '',
            error: ''
        }

        it('should add a gloss term', async () => {
            const fetchResponse = createFetchResponse({ data: gloses, ok: true, status: 200 })
            global.fetch = vi.fn().mockResolvedValue(createFetchResponse(fetchResponse))

            // Type https://medium.com/@turingvang/typescript-2-8-3-type-must-have-a-symbol-iterator-method-that-returns-an-iterator-c240961216d4
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-expect-error
            const response = await addGlossTerm(initialState, formData)
            expect(response).toEqual({
                error: null,
                message: 'Nouveau terme some words ajouté avec succès'
            })
            expect(fetch).toHaveBeenCalledWith(
                'http://localhost:3000/api/gloses',
                expect.objectContaining({
                    method: 'POST',
                    headers: new Headers(),
                    mode: 'cors',
                    body: '{"title":"some words","description":"some words","tags":"some words"}'
                    // objectContaining to fix toMatchObject issue
                })
            )
        })

        describe('with error on inserting', async () => {
            it('should return an error message', async () => {
                const fetchResponse = createFetchResponse({ ok: true, status: 200 })
                global.fetch = vi.fn().mockRejectedValue(createFetchResponse(fetchResponse))

                // Type https://medium.com/@turingvang/typescript-2-8-3-type-must-have-a-symbol-iterator-method-that-returns-an-iterator-c240961216d4
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-expect-error
                const response = await addGlossTerm(initialState, formData)
                expect(response).toEqual({
                    error: "Une erreur est survenue dans l'ajout d'un nouveau terme",
                    message: null
                })
            })
        })
    })

    describe('getAllGlosesTerms', async () => {
        it('should get all gloss terms', async () => {
            const fetchResponse = createFetchResponse({ data: gloses, ok: true, status: 200 })
            global.fetch = vi.fn().mockResolvedValue(createFetchResponse(fetchResponse))
            const response = await getAllGlosesTerms()
            expect(response).toEqual({
                error: null,
                message: null,
                gloses
            })
            expect(fetch).toHaveBeenCalledWith(
                'http://localhost:3000/api/gloses',
                expect.objectContaining({
                    method: 'GET',
                    headers: new Headers(),
                    mode: 'cors'
                })
            )
        })

        describe('with error on inserting', async () => {
            it('should return an error message', async () => {
                const fetchResponse = createFetchResponse({ ok: true, status: 200 })
                global.fetch = vi.fn().mockRejectedValue(createFetchResponse(fetchResponse))

                const response = await getAllGlosesTerms()
                expect(response).toEqual({
                    error: 'Une erreur est survenue en récupérant la liste des gloses',
                    message: null,
                    gloses: []
                })
            })
        })
    })
})