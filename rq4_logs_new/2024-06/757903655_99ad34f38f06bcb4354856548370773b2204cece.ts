import { NextRequest, NextResponse } from 'next/server';
import { Comment, FeedbackComment, LikesComment, User } from '@prisma/client';
import { prisma } from '../..';
import Pusher from 'pusher';
import { cookies } from 'next/headers';
import { revalidateTag } from 'next/cache';
import { Omit, Optional } from '@prisma/client/runtime/library';

export type FeedbackChapter = FeedbackComment & { user: User };

export async function GET(req: NextRequest) {
	const FEEDBACK = prisma.feedbackComment;

	const comment_id = req.nextUrl.searchParams.get('comment_id');
	const feedback_id = req.nextUrl.searchParams.get('feedback_id');

	if (feedback_id && feedback_id != 'undefined') {
		const comments = await FEEDBACK.findMany({
			where: {
				feedback_id: +feedback_id,
			},
			orderBy: {
				created_at: 'desc',
			},
			include: {
				user: true,
			},
		});
		prisma.$disconnect();

		return NextResponse.json(comments);
	}

	if (comment_id && comment_id != 'undefined') {
		const comments = await FEEDBACK.findMany({
			where: {
				comment_id: +comment_id,
			},
			orderBy: {
				created_at: 'desc',
			},
			include: {
				user: true,
			},
		});
		prisma.$disconnect();

		return NextResponse.json(comments);
	}
	prisma.$disconnect();

	return NextResponse.json([]);
}

export type FeedbackPostDTO = {
	content: string;
	feedback_id?: number;
	comment_id?: number;
	user_id: number;
};

export async function POST(req: NextRequest) {
	const { content, feedback_id, comment_id, user_id } = await req.json();

	const FEEDBACK = prisma.feedbackComment;

	await FEEDBACK.create({
		data: {
			content,
			comment_id,
			feedback_id,
			user_id,
		},
		include: {
			user: true,
		},
	});

	const feedbacks = await FEEDBACK.findMany({
		where: {
			comment_id,
			feedback_id,
		},
		orderBy: {
			created_at: 'desc',
		},
		include: {
			user: true,
		},
	});

	prisma.$disconnect();
	revalidateTag('feedback');
	return NextResponse.json(feedbacks);
}

export async function PUT(req: NextRequest) {
	const { comment } = await req.json();
	const user: User =
		cookies().has('user') && JSON.parse(cookies().get('user')!.value);

	if (!user) {
		return NextResponse.json({
			type: 'error',
			message: 'Вы не вошли',
		});
	}

	const COMMENTS = prisma.comment;
	const LIKES = prisma.likesComment;

	const commentFind = await COMMENTS.findFirst({
		where: {
			id: comment.id,
		},
		include: {
			LikesComment: true,
		},
	});

	if (!commentFind) {
		return NextResponse.json({
			type: 'error',
			message: 'Комментарий не найден',
		});
	}

	const likes = commentFind.LikesComment;
	if (likes.length == 0) {
		await LIKES.create({
			data: {
				comment_id: commentFind.id,
				user_id: user.id,
			},
		});
		revalidateTag('chapter');
		revalidateTag('comment');
		return NextResponse.json({
			type: 'message',
			message: 'Лайк добавлен с комментария',
		});
	}

	if (likes.length > 0) {
		LIKES.delete({
			where: {
				id: likes[0].id,
				user_id: user.id,
			},
		});
		revalidateTag('chapter');
		revalidateTag('comment');

		return NextResponse.json({
			type: 'message',
			message: 'Лайк убран с комментария',
		});
	}

	return NextResponse.json({
		type: 'error',
		message: 'Случилось что-то крайне неожиданное',
	});
}

export async function DELETE(req: NextRequest) {
	const COMMENTS = prisma.comment;
	const comment_id = req.nextUrl.searchParams.get('id');

	if (!comment_id) {
		return NextResponse.json({
			type: 'error',
			message: 'Параметр book_id не пришёл',
		});
	}

	await COMMENTS.delete({
		where: {
			id: Number(comment_id),
		},
	});

	return NextResponse.json({
		type: 'message',
		message: 'Параметр book_id не пришёл',
	});
}