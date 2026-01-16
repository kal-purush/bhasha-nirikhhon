import { useState, useEffect } from 'react';
import { User } from '../types/user';
import { getUserByEmail } from './UserMethods';
import { Meteor } from 'meteor/meteor';

export function useCurrentUser() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    try {
      const email: string | undefined = Meteor.user()?.emails?.[0]?.address;

      if (email) {
        getUserByEmail(email).then((user: User) => {
          if (user) {
            setUser(user);
          } else {
            console.log("User not found");
          }
        });
      } else {
        throw new Error("No email found");
      }
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Unknown error'));
    } finally {
      setLoading(false);
    }
  }, []);

  return { user, loading, error };
}