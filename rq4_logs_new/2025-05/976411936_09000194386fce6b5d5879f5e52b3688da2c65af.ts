import { MediaAssetResponseDto } from "./MediaAssetResponseDto";

export interface CategorizedMediaAssets {
  picture: MediaAssetResponseDto[];
  video: MediaAssetResponseDto[];
  floorPlan: MediaAssetResponseDto[];
  vrTour: MediaAssetResponseDto[];
}

export interface ListingCaseDetail {
  id: number;
  title: string;
  // (include other fields from ListingCase)
  mediaAssets: CategorizedMediaAssets;
}