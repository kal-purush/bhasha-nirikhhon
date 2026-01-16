import { getTask, removeTask, updateTask } from "@/lib/repositories/task";
import { NextRequest, NextResponse } from "next/server";

export const GET = async (
  request: NextRequest,
  context: { params: any }
): Promise<NextResponse> => {
  const { id } = context.params;
  try {
    const task = await getTask(id);
    return NextResponse.json(task);
  } catch (err) {
    return new NextResponse("Internal Error", { status: 500 });
  }
};

export const PATCH = async (
  request: NextRequest,
  context: { params: any }
): Promise<NextResponse> => {
  const { text, done } = await request.json();
  const { id } = context.params;
  try {
    const task = await updateTask(id, { text, done });
    if (task?.acknowledged) {
      return new NextResponse("", { status: 200 });
    } else {
      return new NextResponse("Internal Error", { status: 500 });
    }
  } catch (err) {
    return new NextResponse(`Internal Error`, { status: 500 });
  }
};

export const DELETE = async (
  request: NextRequest,
  context: { params: any }
): Promise<NextResponse> => {
  const { id } = context.params;
  try {
    const task = await removeTask(id);
    if (task?.acknowledged) {
      return new NextResponse("", { status: 200 });
    } else {
      return new NextResponse("Internal Error", { status: 500 });
    }
  } catch (err) {
    return new NextResponse("Internal Error", { status: 500 });
  }
};