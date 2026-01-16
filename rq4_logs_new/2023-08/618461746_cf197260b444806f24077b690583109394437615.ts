import { useQuery } from '@tanstack/react-query';

import { api } from '@/constants';
import { outgoingDocument, UploadFile } from '@/models';

const attach: UploadFile[] = [
  new UploadFile({
    name: 'File 1',
    url: 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
    needSigned: false
  }),
  new UploadFile({
    name: 'File 2',
    url: 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
    needSigned: false
  })
];

export const getOne = async (
  id: number
): Promise<outgoingDocument.OutgoingDocument> => {
  const document: outgoingDocument.OutgoingDocument = {
    id: id,
    epitomize: 'Lorem ipsum dolor sit amet',
    archivedBookName: 'Book 1',
    outgoingDocumentNumber: 123,
    documentNotation: 'ABC123',
    publishDate: '2021-10-01T00:00:00.000Z' as unknown as Date,
    receiver: 'john.doe@example.com',
    lastSignedBy: 'Jane Doe',
    createdByDepartment: 'IT',
    createdBy: 'John Doe',
    documentField: 'Field 1',
    documentType: 'Công văn',
    directingDescription: 'Description 1',
    processDeadline: new Date('2021-10-31T00:00:00.000Z'),
    isRepliedDocument: false,
    note: 'Lorem ipsum dolor sit amet',
    status: 'active',
    attachments: attach
  };
  return document;
};
export const useGetOneDocument = (id: number) => {
  return useQuery<outgoingDocument.OutgoingDocument, Error>(
    [api.OUTGOING_DOCUMENT, id],
    () => getOne(id)
  );
};