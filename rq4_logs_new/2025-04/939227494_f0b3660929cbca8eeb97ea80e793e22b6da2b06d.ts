import {
  CACHCES_KEYS,
  createCategorySchema,
  editCategorySchema,
  URL_PATHS,
} from "@/constants";
import { fetchCategories, prisma } from "@/lib";
import { uploadImage } from "@/lib/cloudinary";
import { revalidatePath, revalidateTag } from "next/cache";
import { NextRequest, NextResponse } from "next/server";

export async function GET() {
  try {
    const categories = await fetchCategories();

    return NextResponse.json(categories, { status: 200 });
  } catch (error) {
    console.error("Error fetching categories:", error);
    return NextResponse.json(
      { error: "Failed to fetch categories" },
      { status: 500 }
    );
  }
}

/**
 * accept
 * check
 * add
 * valid
 * return it
 * @returns
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const valid = createCategorySchema.safeParse(body);
    if (!valid.success) {
      return NextResponse.json(
        {
          message: "Please , Send All Data",
          errors: valid.error.flatten().fieldErrors,
        },
        { status: 400 }
      );
    }

    const { imageUrl, name } = valid.data;
    const fullURL = await uploadImage(imageUrl);

    const category = await prisma.category.create({
      data: {
        name,
        imageUrl: fullURL,
      },
    });
    revalidatePath(URL_PATHS.ADMIN.CATEGORIE.HOME_PAGE);
    revalidateTag(CACHCES_KEYS.CATEGORIES);
    return NextResponse.json(category, { status: 200 });
  } catch (error) {
    return NextResponse.json(
      { message: "Failed to Create New  category", error },
      { status: 500 }
    );
  }
}

/**
 * PUT - UPDATA
 * GET DATA
 * CHECK
 * CHECK IMAGE
 *
 * UPDATA
 *
 * revalidatePath
 * RETURN SUSCCFUL
 */

export async function PUT(req: NextRequest) {
  try {
    const body = await req.json();

    const isValid = editCategorySchema.safeParse(body);

    if (!isValid.success) {
      return NextResponse.json(
        {
          message: "Please , Send All Data",
          errors: isValid.error.flatten().fieldErrors,
        },
        { status: 400 }
      );
    }

    const data = isValid.data;

    const oldData = await prisma.category.findUnique({
      where: {
        id: data.id,
      },
    });

    let finalURL = oldData?.imageUrl;
    if (oldData?.imageUrl !== data.imageUrl) {
      finalURL = await uploadImage(data.imageUrl);
    }

    // updata

    await prisma.category.update({
      where: {
        id: data.id,
      },
      data: { ...data, imageUrl: finalURL },
    });

    revalidateTag(CACHCES_KEYS.CATEGORIES);

    return NextResponse.json(
      { message: "Success to Change category Information" },
      { status: 201 }
    );
  } catch (error) {
    return NextResponse.json(
      { message: "Failed to Change category Information", error },
      { status: 500 }
    );
  }
}