import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function main() {
    const roleAdmin = await prisma.role.create({
        data: {
            name: "Admin"
        }
    })
    const roleEditor = await prisma.role.create({
        data: {
            name: "Editor"
        }
    })
    const roleViewer = await prisma.role.create({
        data: {
            name: "Viewer"
        }
    })
    const roleGuest = await prisma.role.create({
        data: {
            name: "Guest"
        }
    })
    const permissionViewUsers = await prisma.permission.create({
        data: {
            name: "viewUsers"
        }
    })
    const permissionEditUsers = await prisma.permission.create({
        data: {
            name: "editUsers"
        }
    })
    const permissionViewRoles = await prisma.permission.create({
        data: {
            name: "viewRoles"
        }
    })
    const permissionEditRoles = await prisma.permission.create({
        data: {
            name: "editRoles"
        }
    })
    const permissionviewProducts = await prisma.permission.create({
        data: {
            name: "viewProducts"
        }
    })
    const permissionEditProducts = await prisma.permission.create({
        data: {
            name: "editProducts"
        }
    })
    const permissionViewOrders = await prisma.permission.create({
        data: {
            name: "viewOrders"
        }
    })
    const permissionEditOrders = await prisma.permission.create({
        data: {
            name: "editOrders"
        }
    })
}

// execute the main function
main()
    .catch((e) => {
        console.error(e);
        process.exit(1);
    })
    .finally(async () => {
        // close the Prisma Client at the end
        await prisma.$disconnect();
    });