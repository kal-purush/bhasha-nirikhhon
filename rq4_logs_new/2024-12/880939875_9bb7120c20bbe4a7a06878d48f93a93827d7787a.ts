import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { getOrCreateUserFromRequest } from "@/lib/auth";
import { AdminService } from "@/services/AdminService";
import { ApiResponse } from "@/lib/api-response";
import { AppError } from "@/lib/errors";
import { AuthErrors } from "@/constants/errors";
import logger from "@/logging";
import { WorkerStatus } from "@prisma/client";

const adminService = new AdminService(prisma);
const DEFAULT_PAGE_SIZE = 25;

type SortField = 'createdAt' | 'lastHeartbeat' | 'status' | 'name';
type SortOrder = 'asc' | 'desc';

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

    // Parse query parameters
    const { searchParams } = new URL(request.url);
    const page = Math.max(1, Number(searchParams.get('page')) || 1);
    const pageSize = Math.max(1, Number(searchParams.get('pageSize')) || DEFAULT_PAGE_SIZE);
    const sortField = (searchParams.get('sortField') as SortField) || 'createdAt';
    const sortOrder = (searchParams.get('sortOrder') as SortOrder) || 'desc';

    // Validate sort field
    const validSortFields: SortField[] = ['createdAt', 'lastHeartbeat', 'status', 'name'];
    if (!validSortFields.includes(sortField)) {
      throw AppError.badRequest('Invalid sort field');
    }

    // Get total count for pagination
    const totalCount = await prisma.workerHeartbeat.count();
    const totalPages = Math.ceil(totalCount / pageSize);

    // Fetch paginated and sorted data
    const heartbeats = await prisma.workerHeartbeat.findMany({
      take: pageSize,
      skip: (page - 1) * pageSize,
      orderBy: {
        [sortField]: sortOrder,
      },
    });

    logger.info(`Fetched ${heartbeats.length} worker heartbeats (page ${page}/${totalPages})`);

    // Return paginated response
    return ApiResponse.success({
      data: heartbeats,
      pagination: {
        currentPage: page,
        totalPages,
        pageSize,
        totalCount,
      },
      sort: {
        field: sortField,
        order: sortOrder,
      },
    });
  } catch (error) {
    return ApiResponse.error(error);
  }
} 