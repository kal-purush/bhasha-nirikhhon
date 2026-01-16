import useFetch, { hasData } from '@nutgaard/use-fetch';
import { Aktivitet, ArenaAktivitet } from '../utils/AktivitetTypes';
import { StringOrNull } from '../utils/Typer';

type mabyAktivitet = Aktivitet | ArenaAktivitet | undefined;

export function useFindAktivitet() {
    const aktiviteterFetch = useFetch<Aktivitet[]>('/veilarbaktivitet/api/aktivitet');
    const arenaAktiviteterFetch = useFetch<ArenaAktivitet[]>('/veilarbaktivitet/api/aktivitet/arena');

    const aktivitetListe = hasData(aktiviteterFetch) ? aktiviteterFetch.data : undefined;
    const arenaListe = hasData(arenaAktiviteterFetch) ? arenaAktiviteterFetch.data : undefined;

    function findAktiivtet(aktivitetId?: StringOrNull): mabyAktivitet {
        if (!aktivitetId) {
            return undefined;
        }

        const aktivitet = aktivitetListe && aktivitetListe.find(a => a.id === aktivitetId);

        return aktivitet ? aktivitet : arenaListe && arenaListe.find(a => a.id === aktivitetId);
    }

    return findAktiivtet;
}