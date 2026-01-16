import { Knex } from 'knex'

export async function seed(knex: Knex): Promise<void> {
    // Deletes ALL existing entries
    await knex('gloses').del()

    // Inserts seed entries
    await knex('gloses').insert([
        {
            created_at: '2025-01-02T15:52:46.963Z',
            description: 'Invented by Alistair Cockburn in 2005',
            id: 1,
            tags: 'Craft, Architecture',
            title: 'Hexagonale architecture'
        },
        {
            created_at: '2024-12-30T15:52:46.963Z',
            description: 'Invented by Eric Evans in 2003',
            id: 2,
            tags: 'Architecture',
            title: 'DDD'
        },
        {
            created_at: '2024-12-29T15:52:46.963Z',
            description:
                'Software engineer and instructor, Robert C. Martin, introduced the collection of principles in his 2000 paper Design Principles and Design Patterns about software rot. The SOLID acronym was coined around 2004 by Michael Feathers.',
            id: 3,
            tags: 'Design, OOP',
            title: 'SOLID'
        }
    ])
}