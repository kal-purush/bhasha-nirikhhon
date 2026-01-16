import { NextRequest, NextResponse } from 'next/server';
import HeaderService from '@/lib/profile/header/HeaderService';

// GET user data, images, and assignments
export async function GET(request: NextRequest) {
	try {
		const searchParams = request.nextUrl.searchParams;
		const userId = searchParams.get('userId');

		if (!userId) {
			return NextResponse.json(
				{ error: 'User ID is required' },
				{ status: 400 }
			);
		}

		const userData = await HeaderService.getUserData(userId);
		const userImages = await HeaderService.getUserImages(userId);
		const imageAssignments = await HeaderService.getImageAssignments(userId);

		return NextResponse.json({
			userData,
			userImages,
			imageAssignments,
		});
	} catch (error) {
		console.error('Error in GET /api/profile/header:', error);
		return NextResponse.json(
			{ error: 'Internal server error' },
			{ status: 500 }
		);
	}
}

// POST to select a profile image
export async function POST(request: NextRequest) {
	try {
		const body = await request.json();
		const { action, userId, data } = body;

		if (!userId || !action) {
			return NextResponse.json(
				{ error: 'User ID and action are required' },
				{ status: 400 }
			);
		}

		let result;

		switch (action) {
			case 'selectProfileImage':
				if (!data.selectedImage) {
					return NextResponse.json(
						{ error: 'Selected image is required' },
						{ status: 400 }
					);
				}
				result = await HeaderService.selectProfileImage(
					userId,
					data.selectedImage
				);
				break;

			case 'uploadProfileImage':
				// Note: File uploads should be handled with formData in a separate route
				return NextResponse.json(
					{ error: 'File uploads should use the /upload endpoint' },
					{ status: 400 }
				);

			case 'saveCoverImage':
				if (!data.selectedImageIds || !Array.isArray(data.selectedImageIds)) {
					return NextResponse.json(
						{ error: 'Selected image IDs array is required' },
						{ status: 400 }
					);
				}
				result = await HeaderService.saveCoverImage(
					userId,
					data.selectedImageIds
				);
				break;

			case 'updateBio':
				if (data.bio === undefined) {
					return NextResponse.json(
						{ error: 'Bio text is required' },
						{ status: 400 }
					);
				}
				result = await HeaderService.updateBio(userId, data.bio);
				break;

			case 'reorderCoverImages':
				if (!data.orderedImageIds || !Array.isArray(data.orderedImageIds)) {
					return NextResponse.json(
						{ error: 'Ordered image IDs array is required' },
						{ status: 400 }
					);
				}
				result = await HeaderService.updateCoverImageOrder(
					userId,
					data.orderedImageIds
				);
				break;

			default:
				return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
		}

		if (result) {
			return NextResponse.json({ success: true });
		} else {
			return NextResponse.json({ error: 'Operation failed' }, { status: 500 });
		}
	} catch (error) {
		console.error('Error in POST /api/profile/header:', error);
		return NextResponse.json(
			{ error: 'Internal server error' },
			{ status: 500 }
		);
	}
}