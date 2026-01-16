import groq from 'groq';
import { client } from '../adminClient';
import { sanityFetch } from '../live';

interface CreateUserProps {
  clerkId: string;
  email: string;
  firstName?: string;
  lastName?: string;
  imageUrl?: string;
  role?: string;
}

export async function createUserIfNotExists({
  clerkId,
  email,
  firstName,
  lastName,
  imageUrl,
  role,
}: CreateUserProps) {
  // First check if user exists
  const existingUserQuery = await sanityFetch({
    query: groq`*[_type == "user" && clerkId == $clerkId][0]`,
    params: { clerkId },
  });

  if (existingUserQuery.data) {
    console.log('User already exists', existingUserQuery.data);
    return existingUserQuery.data;
  }

  // If no user exists, create a new one
  const newUser = await client.create({
    _type: 'user',
    clerkId,
    email,
    firstName,
    lastName,
    imageUrl,
    role,
  });

  console.log('New user created', newUser);

  return newUser;
}