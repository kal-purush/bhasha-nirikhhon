import { PrismaClient } from '@prisma/client';
import { NextResponse } from 'next/server'
import prisma from '@/lib/db'


//@ts-ignore
export async function POST(req, res) {
  if (req.method === 'POST') {
    // Get the event data from the request body
    const { name, associationId, validUntil, locationIP, locationGEO } = await req.json();

    // Find the association in the database
    const association = await prisma.association.findUnique({
      where: { association_id: associationId },
    });

    if (!association) {
      // If the association was not found, send an error response
      res.statusCode = 400;
      res.json({ message: 'Association not found' });
      return;
    }

    // Create a new event in the database
    const event = await prisma.checkIn.create({
      data: {
        name,
        associatedWith: {
          connect: { association_id: associationId },
        },
        expires: addTimeToCurrentDate(validUntil),
        locationIP,
        locationGEO,
      },
    });

    // Send the created event as the response
    return NextResponse.json(event, { status: 201 })

  } else {
    // Handle any other HTTP method
    res.statusCode = 405;
    res.json({ message: 'Method not allowed' });
  }
}


function addTimeToCurrentDate(time: string): string {
  // Create a new Date object for the current date and time
  let currentDate = new Date();

  // Extract the hours and minutes from the time string
  let [hours, minutes] = time.split(':').map(Number);

  // Set the hours and minutes of the current date
  currentDate.setHours(hours);
  currentDate.setMinutes(minutes);

  // Convert the date to UTC and return it in ISO-8601 format
  let dateInISO = currentDate.toISOString();

  // Calculate the offset for Eastern Time Zone (ET)
  // ET is UTC-4 in summer (daylight saving) and UTC-5 in other seasons
  let offset = currentDate.getTimezoneOffset() + 60 * 4; // Change 4 to 5 if not in daylight saving
  offset = offset % (60 * 24); // Make sure the offset is between 0 and 23 hours

  // Apply the offset to get the date and time in ET
  let dateInET = new Date(currentDate.getTime() - offset * 60 * 1000);

  // Return the date in ISO-8601 format
  return dateInET.toISOString();
}