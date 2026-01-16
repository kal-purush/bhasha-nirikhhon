import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

export async function createUser(name: string, email: string) {
    const user = await prisma.user.create({
        data: {
            name: name,
            email: email,
        },
        select: {
            id: true,
            name: true,
            email: true
        }
    })
    return user;
}

export async function getAllUsers() {
    const users = await prisma.user.findMany();
    return users;
}

export function deleteUser(id: string) {
    if (!id) return;

    return prisma.user.delete({
        where: {
            id: id,
        },
    })
}

export function updateUser(id: string, name: string, email: string) {
    return prisma.user.update({
        where: {
            id: id,
        },
        data: {
            name: name,
            email: email
        },
    })
}