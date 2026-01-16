import { useCallback } from "react";
import { FileUploadInfo } from "./useFileUpload";
import { AxiosInstance } from "axios";

type UploadFileFunction = (
  apiClient: AxiosInstance,
  fileUploadInfo: FileUploadInfo
) => void;

type AllFilesUploadInput = {
  apiClient: AxiosInstance;
  laboratoryId: string;
  patientId: string;
  biopsyId: string;
  normalR1File: File | null;
  uploadNormalR1: UploadFileFunction;
  cancelUploadNormalR1: () => void;
  normalR2File: File | null;
  uploadNormalR2: UploadFileFunction;
  cancelUploadNormalR2: () => void;
  tumorR1File: File | null;
  uploadTumorR1: UploadFileFunction;
  cancelUploadTumorR1: () => void;
  tumorR2File: File | null;
  uploadTumorR2: UploadFileFunction;
  cancelUploadTumorR2: () => void;
};

export const useAllFilesUpload = ({
  apiClient,
  laboratoryId,
  patientId,
  biopsyId,
  normalR1File,
  uploadNormalR1,
  cancelUploadNormalR1,
  normalR2File,
  uploadNormalR2,
  cancelUploadNormalR2,
  tumorR1File,
  uploadTumorR1,
  cancelUploadTumorR1,
  tumorR2File,
  uploadTumorR2,
  cancelUploadTumorR2,
}: AllFilesUploadInput) => {
  const startAllFilesUpload = useCallback(async () => {
    const payload = {
      laboratoryId: laboratoryId,
      patientId: patientId,
      biopsyId: biopsyId,
      normalR1FileName: normalR1File?.name,
      normalR1FileSize: normalR1File?.size,
      normalR2FileName: normalR2File?.name,
      normalR2FileSize: normalR2File?.size,
      tumorR1FileName: tumorR1File?.name,
      tumorR1FileSize: tumorR1File?.size,
      tumorR2FileName: tumorR2File?.name,
      tumorR2FileSize: tumorR2File?.size,
    };
    const { data: filesUploadUrls } = await apiClient.request({
      url: "biopsies/upload/get-presigned-urls",
      method: "POST",
      data: payload,
    });

    const NormalR1UploadInfo: FileUploadInfo = filesUploadUrls.NormalR1;
    const NormalR2UploadInfo: FileUploadInfo = filesUploadUrls.NormalR2;
    const TumorR1UploadInfo: FileUploadInfo = filesUploadUrls.TumorR1;
    const TumorR2UploadInfo: FileUploadInfo = filesUploadUrls.TumorR2;

    uploadNormalR1(apiClient, NormalR1UploadInfo);
    uploadNormalR2(apiClient, NormalR2UploadInfo);
    uploadTumorR1(apiClient, TumorR1UploadInfo);
    uploadTumorR2(apiClient, TumorR2UploadInfo);
  }, [
    apiClient,
    laboratoryId,
    patientId,
    biopsyId,
    normalR1File?.name,
    normalR1File?.size,
    normalR2File?.name,
    normalR2File?.size,
    tumorR1File?.name,
    tumorR1File?.size,
    tumorR2File?.name,
    tumorR2File?.size,
    uploadNormalR1,
    uploadNormalR2,
    uploadTumorR1,
    uploadTumorR2,
  ]);

  const cancelAllFileUploads = useCallback(() => {
    cancelUploadNormalR1();
    cancelUploadNormalR2();
    cancelUploadTumorR1();
    cancelUploadTumorR2();
  }, [
    cancelUploadNormalR1,
    cancelUploadNormalR2,
    cancelUploadTumorR1,
    cancelUploadTumorR2,
  ]);

  return { startAllFilesUpload, cancelAllFileUploads };
};