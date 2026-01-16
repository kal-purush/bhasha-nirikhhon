export interface CourseNmProps {
  frtrlNm: string;
  lat: number;
  lot: number;
}

export interface GetFvsnStsfnFrtrlInfoListProps {
  frtrlId: string;
  orgnExmnnPrgrsDrcntNm?: null;
  placeNm: string;
  sgnpstDstn1Nm?: null;
  sgnpstDstn2Nm?: null;
  sgnpstDstn3Nm?: null;
  sgnpstDstn4Nm?: null;
  lot: number;
  orgnPlaceTpeCd: string;
  crtrDt: string;
  aslAltide: number;
  dscrtCn: string;
  orgnPlaceTpeNm: string;
  orgnSgnpstDstn2DrcntNm?: null;
  orgnSgnpstDstn3DrcntCd?: null;
  orgnSgnpstDstn1DrcntNm?: null;
  orgnSgnpstDstn3DrcntNm?: null;
  poiId: string;
  orgnSgnpstDstn4DrcntNm?: null;
  orgnSgnpstDstn1DrcntCd?: null;
  frtrlNm: string;
  lat: number;
  orgnSgnpstDstn2DrcntCd?: null;
  orgnSgnpstDstn4DrcntCd?: null;
}

export interface getBkmtnWalrgCrseInfoListProps {
  orgnRdsfTpe1Nm: string;
  frtrlId: string;
  orgnRdsfTpe2Nm: string;
  orgnRdsfTpe3Cd: string;
  orgnRdsfTpe3Nm: string;
  orgnRdsfTpe1Cd: string;
  orgnRdsfTpe2Cd: string;
  crseId: string;
  lot: number;
  crtrDt: string;
  aslAltide: number;
  frtrlNm: string;
  lat: number;
}

export interface Position {
  lat: number;
  lng: number;
}

export interface Result {
  frtrlNm: string;
  lat: number;
  lot: number;
}

export interface TransformedResult {
  frtrlNm: string;
  position: Position[];
}