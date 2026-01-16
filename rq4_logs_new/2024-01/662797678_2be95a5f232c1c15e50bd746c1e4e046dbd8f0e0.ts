declare module "next-auth" {
  interface DefaultSession {
    user?: {
      id: string;
      name?: string | null;
      email?: string | null;
      image?: string | null;
      // Custom prop to prompt user to change display name
      isInitialLogin?: boolean;
    };
    expires: ISODateString;
  }
}
export { DefaultSession };