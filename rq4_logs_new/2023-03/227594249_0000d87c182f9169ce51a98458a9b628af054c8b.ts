import { getEq } from 'fp-ts/Array';
import * as B from 'fp-ts/lib/boolean';
import { struct } from 'fp-ts/lib/Eq';
import * as N from 'fp-ts/lib/number';
import * as S from 'fp-ts/lib/string';
import { eqNullable, Nullable } from '~src/lib/types';
import {
    Bosituasjon,
    BosituasjongrunnlagRequest,
} from '~src/types/grunnlagsdataOgVilkårsvurderinger/bosituasjon/Bosituasjongrunnlag';
import { NullablePeriode, Periode } from '~src/types/Periode';
import { eqPeriode, lagDatePeriodeAvStringPeriode, lagTomPeriode } from '~src/utils/periode/periodeUtils';
export interface BosituasjonGrunnlagFormData {
export const eqBosituasjonFormItemData = struct<BosituasjonFormItemData>({
    periode: eqNullable(eqPeriode),
    harEPS: eqNullable(B.Eq),
    epsFnr: eqNullable(S.Eq),
    epsAlder: eqNullable(N.Eq),
    delerBolig: eqNullable(B.Eq),
    erEPSUførFlyktning: eqNullable(B.Eq),
});

export const eqBosituasjonGrunnlagFormData = struct<BosituasjonGrunnlagFormData>({
    bosituasjoner: getEq(eqBosituasjonFormItemData),
});

export const nyBosituasjon = (p?: Periode<string>): BosituasjonFormItemData => ({
    periode: p ? lagDatePeriodeAvStringPeriode(p) : lagTomPeriode(),
export const bosituasjongrunnlagTilFormDataEllerNy = (
    b: Bosituasjon[],
    p: Periode<string>
): BosituasjonGrunnlagFormData => ({
    bosituasjoner: b.map((bo) => ({
        periode: lagDatePeriodeAvStringPeriode(bo.periode),
        harEPS: bo.fnr !== null,
        epsFnr: bo.fnr,
        epsAlder: null,
        delerBolig: bo.delerBolig,
        erEPSUførFlyktning: bo.ektemakeEllerSamboerUførFlyktning,
    })) ?? [nyBosituasjon(p)],
});

export const bosituasjongrunnlagFormDataTilRequest = (args: {
    sakId: string;
    behandlingId: string;
    data: BosituasjonGrunnlagFormData;
}): BosituasjongrunnlagRequest => ({
    sakId: args.sakId,
    behandlingId: args.behandlingId,
    vurderinger: args.data.bosituasjoner.map((b) => ({
        periode: {
            fraOgMed: DateUtils.toIsoDateOnlyString(b.periode.fraOgMed!),
            tilOgMed: DateUtils.toIsoDateOnlyString(b.periode.tilOgMed!),
        },
        epsFnr: b.epsFnr,
        delerBolig: b.delerBolig,
        erEPSUførFlyktning: b.erEPSUførFlyktning,
    })),
});

    .object<BosituasjonGrunnlagFormData>({
    BosituasjongrunnlagRequest,
export const lagreBosituasjon = createAsyncThunk<
    Søknadsbehandling,
    GrunnlagOgVilkårApi.BehandlingstypeMedApiRequest<BosituasjongrunnlagRequest>,
    { rejectValue: ApiError }
>('behandling/bosituasjon/ufullstendig', async (arg, thunkApi) => {
    const res = await GrunnlagOgVilkårApi.lagreBosituasjon(arg);
    if (res.status === 'ok') {
        return res.data;
    }
    return thunkApi.rejectWithValue(res.error);
});

export default {
    'page.tittel': 'Bosituasjon & Sats',
};
    Bosituasjon = 'BOSITUASJON',
export interface BosituasjongrunnlagRequest {
    sakId: string;
    behandlingId: string;
    vurderinger: BosituasjonVurderingRequest[];
}

export interface BosituasjonVurderingRequest {
    periode: Periode<string>;
    epsFnr: Nullable<string>;
    delerBolig: Nullable<boolean>;
    erEPSUførFlyktning: Nullable<boolean>;
}

        case Vilkårtype.Bosituasjon:
            return 'Bositausjon & Sats';
        bosituasjon,
        {
            status: VilkårVurderingStatus.IkkeOk,
            vilkårtype: Vilkårtype.Bosituasjon,
            erStartet: bosituasjon !== null,
        },