import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { getOrCreateUserFromRequest } from "@/lib/auth";
import { AdminService } from "@/services/AdminService";
import { OCVVotesService } from "@/services/OCVVotesService";
import { ApiResponse } from "@/lib/api-response";
import { AppError } from "@/lib/errors";
import { AuthErrors } from "@/constants/errors";

const adminService = new AdminService(prisma);
const ocvVotesService = new OCVVotesService(prisma);

export async function GET(request: Request) {
  try {
    const user = await getOrCreateUserFromRequest(request);
    if (!user) {
      throw AppError.unauthorized(AuthErrors.UNAUTHORIZED);
    }

    const isAdmin = await adminService.checkAdminStatus(user.id, user.linkId);
    if (!isAdmin) {
      throw AppError.forbidden(AuthErrors.FORBIDDEN);
    }

    const { searchParams } = new URL(request.url);
    const page = Math.max(1, Number(searchParams.get('page')) || 1);
    const pageSize = Math.max(1, Number(searchParams.get('pageSize')) || 25);
    const sortField = searchParams.get('sortField') || 'updatedAt';
    const sortOrder = (searchParams.get('sortOrder') || 'desc') as 'asc' | 'desc';

    const votes = await ocvVotesService.getOCVVotes(
      page,
      pageSize,
      sortField,
      sortOrder
    );

    return ApiResponse.success(votes);
  } catch (error) {
    return ApiResponse.error(error);
  }
}