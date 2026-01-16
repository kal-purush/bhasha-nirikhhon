// user-profile.interface.ts
export interface Deck {
    _id: string;
    title: string;
    visibility: string;
    is_blocked: boolean;
    createdAt: string;
    updatedAt: string;
  }
  
  export interface UserProfile {
    _id: string;
    name: string;
    email: string;
    role: string;
    isEmailVerified: boolean;
    createdAt: string;
    updatedAt: string;
    decks: Deck[]; // Array of decks associated with the user
  }
  
  export interface UserProfileResponse {
    statuscode: number;
    message: string;
    data: UserProfile;
    success: boolean;
  }
  