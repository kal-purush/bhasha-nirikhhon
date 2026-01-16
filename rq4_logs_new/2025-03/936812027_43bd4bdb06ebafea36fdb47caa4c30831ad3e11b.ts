import { client } from '../adminClient';

export interface UserProps {
  clerkId: string;
  email: string;
  firstName?: string;
  lastName?: string;
  imageUrl?: string;
  role?: string[];
}

export async function createUser({
  clerkId,
  email,
  firstName,
  lastName,
  imageUrl,
  role,
}: UserProps) {
  client
    .create({
      _type: 'user',
      clerkId,
      email,
      firstName,
      lastName,
      imageUrl,
      role,
    })
    .then((res) => {
      console.log(`User was created, user ID is ${res._id}`);
    });
}

export async function updateUser({
  clerkId,
  email,
  firstName,
  lastName,
  imageUrl,
}: UserProps) {
  client
    .patch({
      query: '*[_type == "user" && clerkId == $clerkId]',
      params: { clerkId: clerkId },
    })
    .set({ email, firstName, lastName, imageUrl })
    .commit()
    .then((updatedUser) => {
      console.log('User is updated:', updatedUser);
    })
    .catch((err) => {
      console.log('The update failed:', err.message);
    });
}

export async function deleteUser({ clerkId }: { clerkId: string }) {
  client
    .delete({
      query: '*[_type == "user" && clerkId == $clerkId][0]',
      params: { clerkId: clerkId },
    })
    .then(() => {
      console.log('The user was deleted');
    })
    .catch((err) => {
      console.error('Delete failed:', err.message);
    });
}