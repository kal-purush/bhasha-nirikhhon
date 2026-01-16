import * as Sequelize from 'sequelize';

import InternshipTypes from '../../models/InternshipTypes';

import logger from '../../utils/logger';

import { defaultCategories } from '../data/categories';
import Mentors from '../../models/Mentors';
import { adminsEmail, admins } from '../data/admin';

export default async function() {
    try {
        // Setup admin
        const mentors = await Mentors.findAll({
            where: {
                email: { [Sequelize.Op.in]: adminsEmail },
            },
        });
        const promises = [];
        for (const admin of admins) {
            const found = mentors.findIndex((m) => m.email === admin.email);
            if (found === -1) {
                // Create admin
                promises.push(Mentors.create(admin));
                logger.info(`ADMINS - Inject admin '${admin.email}'`);
            } else {
                // Check if admin definition is OK
                const mentor = mentors[found];
                if (mentor.role !== 'admin') {
                    mentor.set('role', 'admin');
                    promises.push(mentor.save());
                    logger.info(`ADMINS - Update admin '${admin.email}'`);
                }
            }
        }

        // Setup categories
        const categories = await InternshipTypes.findAll();
        if (!categories || !Array.isArray(categories) || !categories.length) {
            // If not any categories, create all default categories
            for (const label of defaultCategories) {
                promises.push(InternshipTypes.create({ label }));
                logger.info(`CATEGORIES - Inject '${label}'`);
            }
        }
        await Promise.all(promises as any);
    } catch (error) {
        // Log error and exit
        logger.error(error);
        process.exit(1);
    }
}