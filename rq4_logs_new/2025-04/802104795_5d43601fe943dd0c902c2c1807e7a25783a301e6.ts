import { Router } from "express";
import { prisma } from "../../prisma/db.setup";
import { validateRequestBody } from "zod-express-middleware";
import { z } from "zod";
import { Decimal } from "@prisma/client/runtime/library";

const showsRouter = Router();

// GET all shows
showsRouter.get("/", async (req, res) => {
  try {
    const shows = await prisma.product.findMany({
      where: {
        isShow: true,
      },
      include: {
        showType: true,
        brand: true,
        category: true,
        showProducts: {
          include: {
            product: {
              include: {
                brand: true,
                category: true,
              },
            },
          },
        },
      },
    });
    return res.status(200).json(shows);
  } catch (error) {
    console.error("Error fetching shows:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// GET shows by type
showsRouter.get("/type/:typeId", async (req, res) => {
  const { typeId } = req.params;
  try {
    const shows = await prisma.product.findMany({
      where: {
        isShow: true,
        showTypeId: typeId,
      },
      include: {
        showType: true,
        brand: true,
        category: true,
        showProducts: {
          include: {
            product: {
              include: {
                brand: true,
                category: true,
              },
            },
          },
        },
      },
    });
    return res.status(200).json(shows);
  } catch (error) {
    console.error("Error fetching shows by type:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// GET a single show
showsRouter.get("/:id", async (req, res) => {
  const { id } = req.params;
  try {
    const show = await prisma.product.findFirst({
      where: {
        id,
        isShow: true,
      },
      include: {
        showType: true,
        brand: true,
        category: true,
        colors: true,
        effects: true,
        showProducts: {
          include: {
            product: {
              include: {
                brand: true,
                category: true,
                unitProduct: true,
                colors: true,
                effects: true,
              },
            },
          },
        },
      },
    });

    if (!show) {
      return res.status(404).json({ message: "Show not found" });
    }

    return res.status(200).json(show);
  } catch (error) {
    console.error("Error fetching show:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Create a new show
const createShowSchema = z.object({
  title: z.string(),
  description: z.string().optional(),
  price: z.number().positive(),
  image: z.string().optional(),
  videoURL: z.string().optional(),
  inStock: z.boolean().optional(),
  showTypeId: z.string(),
  brandId: z.string(),
  categoryId: z.string(),
  products: z.array(
    z.object({
      productId: z.string(),
      quantity: z.number().int().positive(),
      notes: z.string().optional(),
    })
  ),
});

showsRouter.post(
  "/",
  validateRequestBody(createShowSchema),
  async (req, res) => {
    const { products, ...showData } = req.body;
    try {
      const show = await prisma.$transaction(async (tx) => {
        const newShow = await tx.product.create({
          data: {
            ...showData,
            isShow: true,
            package: [1], // Set a default package for the show
            casePrice: new Decimal(showData.price),
            sku: await generateUniqueShowSku(tx),
          },
        });

        for (const product of products) {
          await tx.showProduct.create({
            data: {
              showId: newShow.id,
              productId: product.productId,
              quantity: product.quantity,
              notes: product.notes,
            },
          });
        }

        // Return the show with its products
        return tx.product.findUnique({
          where: { id: newShow.id },
          include: {
            showType: true,
            brand: true,
            category: true,
            showProducts: {
              include: {
                product: true,
              },
            },
          },
        });
      });

      return res.status(201).json(show);
    } catch (error) {
      console.error("Error creating show:", error);
      return res
        .status(500)
        .json({ message: "Internal server error", error: String(error) });
    }
  }
);

async function generateUniqueShowSku(tx: any) {
  // Find the highest show number by extracting the numeric part from existing SKUs
  const showProducts = await tx.product.findMany({
    where: {
      isShow: true,
      sku: {
        startsWith: "SHOW-",
      },
    },
    select: { sku: true },
  });

  // Extract the numeric parts and find the highest one
  const showNumbers = showProducts
    .map((product: { sku: string }) => {
      const match = product.sku.match(/SHOW-(\d+)/);
      return match ? parseInt(match[1], 10) : 0;
    })
    .filter((num: number) => !isNaN(num));

  const highestNumber = showNumbers.length > 0 ? Math.max(...showNumbers) : 0;
  const nextNumber = highestNumber + 1;

  // Generate the new SKU in the format "SHOW-n"
  const newSku = `SHOW-${nextNumber}`;

  return newSku;
}

// Update a show
showsRouter.put("/:id", async (req, res) => {
  const { id } = req.params;
  const { products, ...showData } = req.body;

  try {
    // Start a transaction to handle the update atomically
    const updatedShow = await prisma.$transaction(async (tx) => {
      // First, update the show details
      const updatedShowData = await tx.product.update({
        where: {
          id,
          isShow: true,
        },
        data: {
          ...showData,
          casePrice: new Decimal(showData.price),
        },
      });

      // If products are provided, update the show products
      if (products && Array.isArray(products)) {
        // Delete existing relationships
        await tx.showProduct.deleteMany({
          where: { showId: id },
        });

        // Create new relationships
        for (const product of products) {
          await tx.showProduct.create({
            data: {
              showId: id,
              productId: product.productId,
              quantity: product.quantity,
              notes: product.notes || null,
            },
          });
        }
      }

      // Return the updated show
      return tx.product.findUnique({
        where: { id },
        include: {
          showType: true,
          brand: true,
          category: true,
          showProducts: {
            include: {
              product: true,
            },
          },
        },
      });
    });

    return res.status(200).json(updatedShow);
  } catch (error) {
    console.error("Error updating show:", error);
    return res
      .status(500)
      .json({ message: "Internal server error", error: String(error) });
  }
});

// Delete a show
showsRouter.delete("/:id", async (req, res) => {
  const { id } = req.params;

  try {
    // Verify this is a show
    const show = await prisma.product.findFirst({
      where: {
        id,
        isShow: true,
      },
    });

    if (!show) {
      return res.status(404).json({ message: "Show not found" });
    }

    await prisma.$transaction(async (tx) => {
      await tx.showProduct.deleteMany({
        where: { showId: id },
      });

      await tx.product.delete({
        where: { id },
      });
    });

    return res.status(204).send();
  } catch (error) {
    console.error("Error deleting show:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// GET all show types
showsRouter.get("/types/all", async (req, res) => {
  try {
    const types = await prisma.showType.findMany();
    return res.status(200).json(types);
  } catch (error) {
    console.error("Error fetching show types:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Create a show type
showsRouter.post("/types", async (req, res) => {
  const { name, description } = req.body;

  try {
    const showType = await prisma.showType.create({
      data: {
        name,
        description,
      },
    });
    return res.status(201).json(showType);
  } catch (error) {
    console.error("Error creating show type:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Update a show type
showsRouter.put("/types/:id", async (req, res) => {
  const { id } = req.params;
  const { name, description } = req.body;

  try {
    const showType = await prisma.showType.update({
      where: { id },
      data: {
        name,
        description,
      },
    });
    return res.status(200).json(showType);
  } catch (error) {
    console.error("Error updating show type:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Delete a show type
showsRouter.delete("/types/:id", async (req, res) => {
  const { id } = req.params;

  try {
    // Check if there are any shows using this type
    const showsUsingType = await prisma.product.count({
      where: {
        isShow: true,
        showTypeId: id,
      },
    });

    if (showsUsingType > 0) {
      return res.status(400).json({
        message:
          "Cannot delete this show type because it is being used by existing shows",
      });
    }

    // Delete the show type
    await prisma.showType.delete({
      where: { id },
    });

    return res.status(204).send();
  } catch (error) {
    console.error("Error deleting show type:", error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

export { showsRouter };