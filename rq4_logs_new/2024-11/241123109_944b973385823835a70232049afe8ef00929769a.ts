import { ConsulterCandidatureReadModel, TypeGarantiesFinancières } from '../candidature';
  typeGarantiesFinancières?: TypeGarantiesFinancières.ValueType;
  typeGarantiesFinancières,
  typeGarantiesFinancières: typeGarantiesFinancières
    ? TypeGarantiesFinancières.convertirEnValueType(typeGarantiesFinancières)
    : undefined,
import { getRoleBasedWhereCondition, Utilisateur } from '../../_utils/getRoleBasedWhereCondition';
      payload: { identifiantProjet },
        const {
          payload: { typeTâchePlanifiée },
        } = event;
        if (
          TypeTâchePlanifiéeGarantiesFinancières.convertirEnValueType(
            typeTâchePlanifiée,
          ).estÉchoir()
        ) {
  type?: Candidature.TypeGarantiesFinancières.ValueType;
  if (!type) {
    return;

import { IdentifiantProjet } from '@potentiel-domain/common';
import { loadLauréatFactory } from '../../../lauréat.aggregate';
  const loadLauréat = loadLauréatFactory(loadAggregate);
  const loadCandidature = Candidature.Aggregate.loadCandidatureFactory(loadAggregate);

    const candidature = await loadCandidature(identifiantProjet, false);
    const lauréat = await loadLauréat(identifiantProjet, false);
      importéLe: lauréat.notifiéLe,
      type: candidature.garantiesFinancières?.type,
      dateÉchéance: candidature.garantiesFinancières?.dateEchéance,
import { IdentifiantProjet } from '@potentiel-domain/common';
      data: { identifiantProjet },
export * as TypeGarantiesFinancièresSaga from './typeGarantiesFinancières.saga';

// utils
export * from './_utils/appelOffreSoumisAuxGarantiesFinancières';
import { getRoleBasedWhereCondition, Utilisateur } from '../../_utils/getRoleBasedWhereCondition';
import { getRoleBasedWhereCondition, Utilisateur } from '../../_utils/getRoleBasedWhereCondition';
import { Message, MessageHandler, mediator } from 'mediateur';

import { LauréatNotifiéEvent } from '../lauréat';

import { ImporterTypeGarantiesFinancièresUseCase } from '.';

export type SubscriptionEvent = LauréatNotifiéEvent;

export type Execute = Message<
  'System.Lauréat.TypeGarantiesFinancières.Saga.Execute',
  SubscriptionEvent
>;

export const register = () => {
  const handler: MessageHandler<Execute> = async (event) => {
    const {
      payload: { identifiantProjet },
    } = event;
    switch (event.type) {
      case 'LauréatNotifié-V1':
        await mediator.send<ImporterTypeGarantiesFinancièresUseCase>({
          type: 'Lauréat.GarantiesFinancières.UseCase.ImporterTypeGarantiesFinancières',
          data: {
            identifiantProjetValue: identifiantProjet,
          },
        });
        break;
    }
  };
  mediator.register('System.Lauréat.TypeGarantiesFinancières.Saga.Execute', handler);
};
import { appelOffreSoumisAuxGarantiesFinancières } from './garantiesFinancières/_utils/appelOffreSoumisAuxGarantiesFinancières';
import { DateTime, IdentifiantProjet } from '@potentiel-domain/common';
import { AjouterTâchePlanifiéeCommand } from '@potentiel-domain/tache-planifiee';
  | GarantiesFinancières.GarantiesFinancièresEnregistréesEvent
  | GarantiesFinancières.TypeGarantiesFinancièresImportéEvent;
      case 'TypeGarantiesFinancièresImporté-V1':
        const {
          payload: { dateÉchéance },
        } = event;

        if (dateÉchéance) {
          await mediator.send<AjouterTâchePlanifiéeCommand>({
            type: 'System.TâchePlanifiée.Command.AjouterTâchePlanifiée',
            data: {
              identifiantProjet: IdentifiantProjet.convertirEnValueType(identifiantProjet),
              tâches: [
                {
                  typeTâchePlanifiée:
                    GarantiesFinancières.TypeTâchePlanifiéeGarantiesFinancières.échoir.type,
                  àExécuterLe: DateTime.convertirEnValueType(dateÉchéance).ajouterNombreDeJours(1),
                },
                {
                  typeTâchePlanifiée:
                    GarantiesFinancières.TypeTâchePlanifiéeGarantiesFinancières.rappelÉchéanceUnMois
                      .type,
                  àExécuterLe: DateTime.convertirEnValueType(dateÉchéance).retirerNombreDeMois(1),
                },
                {
                  typeTâchePlanifiée:
                    GarantiesFinancières.TypeTâchePlanifiéeGarantiesFinancières
                      .rappelÉchéanceDeuxMois.type,
                  àExécuterLe: DateTime.convertirEnValueType(dateÉchéance).retirerNombreDeMois(2),
                },
              ],
            },
          });
        }
        break;