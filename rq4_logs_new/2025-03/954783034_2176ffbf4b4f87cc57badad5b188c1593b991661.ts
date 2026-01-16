import { PrismaClient } from "@prisma/client";
import * as XLSX from "xlsx";
import fs from "fs";

const prisma = new PrismaClient();

// Define TypeScript interface for expected Excel row structure
interface ExcelRow {
  "School Name": string;
  "Address": string;
  "City": string;
  "Facilities": string;
}

// Define TypeScript interface for School model in Prisma
interface School {
  name: string;
  address: string;
  city: string;
  facilities: string;
}

// Function to read Excel file and import data into Prisma
async function importSchools() {
  const filePath = "./schools.xlsx"; // Ensure this file exists in your root folder

  if (!fs.existsSync(filePath)) {
    console.error("❌ Error: File not found!");
    return;
  }

  // Read Excel file
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0]; // Read first sheet
  const sheet = workbook.Sheets[sheetName];

  // Convert sheet data to JSON
  const data: ExcelRow[] = XLSX.utils.sheet_to_json<ExcelRow>(sheet);

  // Transform data into Prisma format
  const schools: School[] = data.map((row) => ({
    name: row["School Name"] || "Unnamed School",
    address: row["Address"] || "No Address",
    city: row["City"] || "Unknown City",
    facilities: row["Facilities"] || "Not Available",
  }));

  // Insert data into Prisma
  try {
    await prisma.school.createMany({
      data: schools,
      skipDuplicates: true, // Prevent duplicate entries
    });
    console.log(`✅ Successfully imported ${schools.length} schools!`);
  } catch (error) {
    console.error("❌ Error inserting data into Prisma:", error);
  } finally {
    await prisma.$disconnect();
  }
}

// Run the function
importSchools();