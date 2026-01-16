import { useMutation, useQueryClient } from '@tanstack/react-query';

import { api } from '@/constants';
import { common, UploadFile } from '@/models';
import { CreateType } from '@/types/validation/outgoingDocument';
import { axiosInstance } from '@/utils';

type AttachmentType = {
  name: string;
  url: string;
  needSigned: boolean;
};

type OutGoingDocumentUploadFormType = {
  epitomize: string;
  documentField: string;
  documentTypeId: number;
  isRepliedDocument: boolean;
  note: string;
  attachments: AttachmentType[];
};

type AttachmentTypeResponse = AttachmentType & {
  id: number;
};

type OutGoingDocumentUploadFormTypeResponse = {
  data: Omit<OutGoingDocumentUploadFormType, 'attachments'> & {
    id: number;
    createdBy: number | null;
    createdDate: string;
    createdById: number;
    updatedDate: string;
    updatedBy: number;
    isDeleted: boolean;
  };
  attachments: AttachmentTypeResponse[];
};

type NameAndUrlFile = {
  name: string;
  url: string;
};
type UploadFileResponse = {
  data: NameAndUrlFile[];
};

const convertToOutGoingDocumentUploadFormType = (
  createObj: CreateType
): OutGoingDocumentUploadFormType => {
  const outGoingDocumentUploadFormType: OutGoingDocumentUploadFormType = {
    epitomize: createObj.epitomize,
    documentField: String(createObj.documentField),
    documentTypeId: createObj.documentTypeId,
    isRepliedDocument: createObj.isRepliedDocument ?? false,
    note: createObj.note,
    attachments:
      createObj.files?.map((file) => ({
        name: file.name ?? '',
        url: file.url ?? '',
        needSigned: file.needSigned
      })) ?? []
  };

  return outGoingDocumentUploadFormType;
};

export const uploadFile = async (
  payload: UploadFile[]
): Promise<NameAndUrlFile[]> => {
  const url = 'api/upload';

  const formData = new FormData();

  for (let i = 0; i < payload.length; i++) {
    if (payload[i] && payload[i].fileObj) {
      formData.append('files', payload[i]?.fileObj);
    }
  }

  const result: UploadFileResponse = await axiosInstance.post(url, formData, {
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });

  return result.data;
};

export const uploadForm = async (
  formData: CreateType
): Promise<OutGoingDocumentUploadFormTypeResponse> => {
  const url = 'api/OutgoingDocument';

  const { files: formFiles } = formData;

  if (!formFiles) throw new Error('File is required');

  const uploadedFile = await uploadFile(formFiles);

  for (let idx = 0; idx < uploadedFile.length; idx++) {
    formFiles[idx].setNameAndUrl(uploadedFile[idx].name, uploadedFile[idx].url);
  }

  const payload = convertToOutGoingDocumentUploadFormType(formData);

  console.log(payload);

  return await axiosInstance.post(url, payload);
};

export const useUploadForm = ({
  onSuccess,
  onError
}: common.useMutationParams) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload: CreateType) => uploadForm(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.DEPARTMENT] });
      onSuccess?.();
    },
    onError: () => {
      onError?.();
    }
  });
};