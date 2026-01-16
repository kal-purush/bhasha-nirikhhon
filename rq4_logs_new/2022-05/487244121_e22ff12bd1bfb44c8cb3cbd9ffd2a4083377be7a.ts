import {
  Enum_Photo_Camera,
  PhotoEntity,
} from "../graphql/generated/graphql-types.generated";

export const cameraToEnglish = (camera: Enum_Photo_Camera) => {
  const englishCameras = {
    [Enum_Photo_Camera.NikonFe2]: "Nikon FE2",
  };
  return englishCameras[camera];
};