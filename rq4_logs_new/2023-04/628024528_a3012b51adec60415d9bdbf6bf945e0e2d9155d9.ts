import { PrismaClient } from '@prisma/client';
import * as fs from 'fs'
import { parse } from 'papaparse';

const prisma = new PrismaClient();

export const seedPermission = async () => {
    const data = fs.readFileSync('./prisma/seed_csv_files/permission.csv', 'utf8');
    const csvData = data.toString();
    const parsedCsvData = parse(csvData).data;
    if (parsedCsvData) {
        for (let index = 0; index < parsedCsvData.length; index++) {
            if (index === 0 || parsedCsvData[index] == '')
                continue;
            const permission: any = parsedCsvData[index];
            const permissions = await prisma.permission.create({
                data: {
                    id: parseInt(permission[0]),
                    name: permission[1]
                },
            });
        }
    }
}