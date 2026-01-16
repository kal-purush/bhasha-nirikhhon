    førsteRegistrerteUttaksdag: Date;
    sisteUttaksdagInnenforSeksUker: Date;
    const førsteRegistrerteUttaksdag = førstePeriode.tidsperiode.fom;
    const sisteUttaksdagInnenforSeksUker = getTidsperiode(familiehendelsesdato, 30).tom;
    const antallUttaksdager =
        Tidsperioden({
            fom: familiehendelsesdato,
            tom: førsteRegistrerteUttaksdag
        }).getAntallUttaksdager() - 1;
        førsteRegistrerteUttaksdag,
        sisteUttaksdagInnenforSeksUker
const getSkalInformereOmNårPeriodenStarterÅLøpe = (søkerErFarEllerMedmor: boolean, morHarRett: boolean): boolean => {
    return søkerErFarEllerMedmor && morHarRett === false;
};