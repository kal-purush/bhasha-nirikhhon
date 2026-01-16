// This file can be used to seed initial languages and translations
// Run with: npm run seed:translations

import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function seedTranslations() {
  try {
    console.log("🌱 Seeding languages...");

    // Create English language (default)
    const englishLanguage = await prisma.language.upsert({
      where: { code: "en" },
      update: {},
      create: {
        code: "en",
        name: "English",
        isDefault: true,
        isActive: true,
      },
    });

    // Create Georgian language
    const georgianLanguage = await prisma.language.upsert({
      where: { code: "ka" },
      update: {},
      create: {
        code: "ka",
        name: "ქართული",
        isDefault: false,
        isActive: true,
      },
    });

    console.log("✅ Languages seeded successfully:", {
      english: englishLanguage.code,
      georgian: georgianLanguage.code,
    });

    // Add your translations here when needed
    // Example structure:
    // const translations = [
    //   { key: 'common.welcome', value: 'Welcome', languageId: englishLanguage.id, namespace: 'common' },
    //   { key: 'common.welcome', value: 'მოგესალმებით', languageId: georgianLanguage.id, namespace: 'common' },
    // ];
    //
    // await prisma.translation.createMany({
    //   data: translations,
    //   skipDuplicates: true,
    // });
  } catch (error) {
    console.error("❌ Error seeding translations:", error);
    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

// Run the seed function if this file is executed directly
if (require.main === module) {
  seedTranslations().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export default seedTranslations;