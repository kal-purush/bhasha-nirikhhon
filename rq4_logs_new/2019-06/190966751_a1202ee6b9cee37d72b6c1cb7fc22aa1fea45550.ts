// Average weight for age at 50th percentile
// Sourced from The World Health Organization (WHO), Gerontology Research Center (National Institutes of Health, USA),
// and the U.S. Centers for Disease Control and Prevention (CDC). Published 2000

export class AgeWeight {
    male?: boolean;
    age: number;
    weight: number;
}

export const ageWeight: AgeWeight[] = [
    // male
    {
        male: true,
        age: 1,
        weight: 9.6
    },
    {
        male: true,
        age: 2,
        weight: 12.5
    },
    {
        male: true,
        age: 3,
        weight: 14
    },
    {
        male: true,
        age: 4,
        weight: 16.3
    },
    {
        male: true,
        age: 5,
        weight: 18.4
    },
    {
        male: true,
        age: 6,
        weight: 22.9
    },
    {
        male: true,
        age: 7,
        weight: 22.9
    },
    {
        male: true,
        age: 8,
        weight: 25.6
    },
    {
        male: true,
        age: 9,
        weight: 28.6
    },
    {
        male: true,
        age: 10,
        weight: 32
    },
    {
        male: true,
        age: 11,
        weight: 35.6
    },
    {
        male: true,
        age: 12,
        weight: 39.9
    },
    {
        male: true,
        age: 13,
        weight: 45.3
    },
    {
        male: true,
        age: 14,
        weight: 50.8
    },
    {
        male: true,
        age: 15,
        weight: 56
    },
    {
        male: true,
        age: 16,
        weight: 60.8
    },
    {
        male: true,
        age: 17,
        weight: 64.4
    },
    {
        male: true,
        age: 18,
        weight: 66.9
    },
    {
        male: true,
        age: 19,
        weight: 68.9
    },
    {
        male: true,
        age: 20,
        weight: 70.3
    },
    // female
    {
        age: 1,
        weight: 9.2
    },
    {
        age: 2,
        weight: 12
    },
    {
        age: 3,
        weight: 14.2
    },
    {
        age: 4,
        weight: 15.4
    },
    {
        age: 5,
        weight: 17.9
    },
    {
        age: 6,
        weight: 19.9
    },
    {
        age: 7,
        weight: 22.4
    },
    {
        age: 8,
        weight: 25.8
    },
    {
        age: 9,
        weight: 28.1
    },
    {
        age: 10,
        weight: 31.9
    },
    {
        age: 11,
        weight: 36.9
    },
    {
        age: 12,
        weight: 41.5
    },
    {
        age: 13,
        weight: 45.8
    },
    {
        age: 14,
        weight: 47.6
    },
    {
        age: 15,
        weight: 52.1
    },
    {
        age: 16,
        weight: 53.5
    },
    {
        age: 17,
        weight: 54.4
    },
    {
        age: 18,
        weight: 56.7
    },
    {
        age: 19,
        weight: 57.1
    },
    {
        age: 20,
        weight: 58
    }
];