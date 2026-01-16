import { skipToken } from '@reduxjs/toolkit/query';
import { useHasRole } from '@app/hooks/use-has-role';
import { useIsRol } from '@app/hooks/use-is-rol';
import { useGetDocumentsQuery } from '@app/redux-api/oppgaver/queries/documents';
import { Role } from '@app/types/bruker';
import { ISmartDocument } from '@app/types/documents/documents';
import { TemplateIdEnum } from '@app/types/smart-editor/template-enums';

export const useSmartDocuments = (oppgaveId: string | typeof skipToken): ISmartDocument[] | undefined => {
  const { data, isLoading } = useGetDocumentsQuery(oppgaveId);
  const isRol = useIsRol();
  const hasSaksbehandlerRole = useHasRole(Role.KABAL_SAKSBEHANDLING);

  if (isLoading || typeof data === 'undefined') {
    return undefined;
  }

  const documents: ISmartDocument[] = [];

  if (hasSaksbehandlerRole) {
    for (const document of data) {
      if (document.isSmartDokument && document.templateId !== TemplateIdEnum.ROL_ANSWERS) {
        documents.push(document);
      }
    }
  }

  if (isRol) {
    for (const document of data) {
      if (document.isSmartDokument && document.templateId === TemplateIdEnum.ROL_ANSWERS) {
        documents.push(document);
      }
    }
  }

  return documents;
};