declare interface CompleteYourProfileFormValues {
  profileImage: File | null;
  aboutMe: string;
  serviceDescription: string;
  selectedCategories: string[];
  bankName: string;
  bankCode: string;
  accountNumber: string;
  accountName: string;
  guarantorsName: string;
  guarantorsPhoneNumber: string;
  guarantorsRelationship: string;
  YOE: string;
  days: string;
  startTime: string;
  endTime: string;
  priceRange: string;
}

declare interface verificationAndIdentificationFormValues {
  idType: string;
  certificationsType: string;
  idImage: File | null;
  certificationsImage: File | null;
}

declare interface porfolioFormValues {
  workImage: File | null;
  projectDescription: string;
  tags: string[];
}