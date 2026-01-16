import {
  Prisma,
  RoleEnum,
  StatusEnum,
  Role,
  PrismaClient,
} from '@prisma/client';
import * as bcrypt from 'bcrypt';

export const ADMIN_EMAIL = 'admin@gmail.com';
export const ADMIN_PASSWORD = 'admin';

const prisma = new PrismaClient();

export interface IBaseDataSeed {
  roles: Role[];
}

const baseSeed = async (prisma: PrismaClient): Promise<IBaseDataSeed> => {
  console.log('Seeding base...');

  const baseData: IBaseDataSeed = {
    roles: [],
  };

  const roles = await seedRoles(prisma);

  Object.assign(baseData, { roles });

  return baseData;
};

const seedRoles = async (prisma: PrismaClient): Promise<Role[]> => {
  console.log('Seeding roles...');

  return await prisma.$transaction(
    async (transaction: Prisma.TransactionClient) => {
      const operations = Object.entries(RoleEnum).map(async ([name]) => {
        return await transaction.role.upsert({
          where: { name: name as RoleEnum },
          create: { name: name as RoleEnum, status: StatusEnum.ACTIVE },
          update: { name: name as RoleEnum, status: StatusEnum.ACTIVE },
        });
      });

      return Promise.all(operations);
    },
    {
      maxWait: 30000,
      timeout: 30000,
    },
  );
};

const seedUser = async (
  prisma: PrismaClient,
  baseDataSeed: IBaseDataSeed,
): Promise<void> => {
  console.log('Seeding user');

  const alreadyHasAdminUser =
    (await prisma.user.count({
      where: {
        Role: {
          name: RoleEnum.ADMIN,
        },
      },
    })) > 0;

  if (!alreadyHasAdminUser) {
    console.log('Creating admin user');

    const roleAdminId = baseDataSeed.roles.find(
      role => role.name === RoleEnum.ADMIN,
    ).id;

    await prisma.user.create({
      data: {
        email: ADMIN_EMAIL,
        name: 'Admin User',
        telephone: '(00) 90000-0000',
        Address: {
          create: {
            cep: '00000-000',
            city: 'City',
            neighborhood: 'Neighborhood',
            number: '0',
            street: 'Street',
            uf: 'UF',
            complement: 'Complement',
          },
        },
        Media: {
          create: {
            url: 'https://example.com/image.jpg',
          },
        },
        password: await bcrypt.hash(ADMIN_PASSWORD, 10),
        status: StatusEnum.ACTIVE,
        recoveryPasswordToken: null,
        refreshToken: null,
        deletedAt: null,
        version: 1,
        Role: {
          connect: {
            id: roleAdminId,
          },
        },
      },
    });
  }
};

(async () => {
  const baseData: IBaseDataSeed = await baseSeed(prisma);

  const isTestEnviroment = process.env.ENV == 'TEST';

  if (isTestEnviroment) {
    // Seed Tests
  } else {
    await seedUser(prisma, baseData);
  }
})();