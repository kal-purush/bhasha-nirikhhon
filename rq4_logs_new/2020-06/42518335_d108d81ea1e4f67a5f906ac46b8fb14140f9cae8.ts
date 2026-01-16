import { Computed } from '@shared/utils/interfaces';
import { OpetussuunnitelmaMuokkaustietoDto } from '@shared/api/amosaa';
import { IEsitysnimi } from '@shared/utils/kayttaja';

export interface IMuokkaustietoProvider {
  muokkaustiedot: Computed<OpetussuunnitelmaMuokkaustietoDto[]>;
  viimeinenHaku: Computed<OpetussuunnitelmaMuokkaustietoDto[]>;
  hakuLukumaara: Computed<number>;
  update: () => Promise<void>;
}

export interface Muokkaustieto {
  id?: number;
  nimi?: { [key: string]: string; };
  tapahtuma?: string;
  parentId?: number;
  kohdeId?: number;
  kohde?: string;
  luotu?: Date;
  muokkaaja?: string;
  lisatieto?: string;
  poistettu?: boolean;
  kayttajanTieto?: IEsitysnimi;
}