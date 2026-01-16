// _id: 1282724,
//     mispar_rechev: 6678759,
//     tozeret_cd: '0751',
//     sug_degem: 'P',
//     tozeret_nm: 'שברולט ד.קוריא',
//     degem_cd: '0061',
//     degem_nm: 'JJ526',
//     ramat_gimur: 'LS',
//     ramat_eivzur_betihuty: null,
//     kvutzat_zihum: null,
//     shnat_yitzur: '2005',
//     degem_manoa: 'F16D3',
//     mivchan_acharon_dt: '2022-05-23',
//     tokef_dt: '2023-04-18',
//     baalut: 'פרטי',
//     misgeret: 'KL1JJ52685K141451',
//     tzeva_cd: '36',
//     tzeva_rechev: 'כחול מטל',
//     zmig_kidmi: '195\\55R15',
//     zmig_ahori: '195\\55R15',
//     sug_delek_nm: 'בנזין',
//     horaat_rishum: null,
//     moed_aliya_lakvish: null,
//     kinuy_mishari: 'OPTRA',
//     rank: 0.0573088

export type CarDataKeys = {
  key: string;
  label: string;
};

const generalSpecs: CarDataKeys[] = [
  {
    key: "mivchan_acharon_dt",
    label: "טסט אחרון",
  },
  {
    key: "tokef_dt",
    label: "תוקף עד",
  },
  {
    key: "ramat_gimur",
    label: "רמת גימור",
  },
  {
    key: "ramat_eivzur_betihuty",
    label: "רמת אבזור בטיחותי",
  },
  {
    key: "tzeva_rechev",
    label: "צבע הרכב",
  },
  {
    key: "baalut",
    label: "סוג בעלות",
  },
  {
    key: "moed_aliya_lakvish",
    label: "תאריך עלייה לכביש",
  },
];

const techSpecs: CarDataKeys[] = [
  {
    key: "degem_manoa",
    label: "מספר שלדה",
  },
  {
    key: "degem_manoa",
    label: "דגם מנוע",
  },
  {
    key: "zmig_kidmi",
    label: "צמיג קדמי",
  },
  {
    key: "zmig_ahori",
    label: "צמיג אחורי",
  },
  {
    key: "sug_delek_nm",
    label: "סוג דלק",
  },
];

export { generalSpecs, techSpecs };