'use server';

import { checkUser } from '@/helpers/auth';
import { prisma } from '@/lib/prisma';
import { createFormSchema, CreateFormSchema } from '@/schemas/CreateForm';

export const getFormStatus = async () => {
	const { id: userId } = await checkUser();

	const stats = await prisma.form.aggregate({
		where: {
			userId,
		},
		_sum: { visits: true, submissions: true },
	});

	const visits = stats._sum.visits || 0;
	const submissions = stats._sum.submissions || 0;
	const submissionRate = visits > 0 ? (submissions / visits) * 100 : 0;
	const bounceRate = 100 - submissionRate;

	return { visits, submissions, submissionRate, bounceRate };
};

export const createForm = async (values: CreateFormSchema) => {
	const { id: userId } = await checkUser();

	const validation = createFormSchema.safeParse(values);
	if (validation.error) {
		throw new Error('Validation error');
	}

	const { name, description } = values;
	const form = await prisma.form.create({
		data: { userId, name, description },
	});

	if (!form) {
		throw new Error('Something went wrong');
	}

	return form;
};

export const getForms = async () => {
	const { id: userId } = await checkUser();

	return await prisma.form.findMany({ where: { userId }, orderBy: { createdAt: 'desc' } });
};