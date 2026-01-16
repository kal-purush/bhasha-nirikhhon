export interface IBooks {
  kind?: string;
  totalItems?: number;
  items?: IBook[];
}

export interface IBook {
  kind?: Kind;
  id?: string;
  etag?: string;
  selfLink?: string;
  volumeInfo?: VolumeInfo;
  saleInfo?: SaleInfo;
  accessInfo?: AccessInfo;
  searchInfo?: SearchInfo;
}

export interface AccessInfo {
  country?: Country;
  viewability?: Viewability;
  embeddable?: boolean;
  publicDomain?: boolean;
  textToSpeechPermission?: TextToSpeechPermission;
  epub?: Epub;
  pdf?: Epub;
  webReaderLink?: string;
  accessViewStatus?: AccessViewStatus;
  quoteSharingAllowed?: boolean;
}

export enum AccessViewStatus {
  FullPublicDomain = 'FULL_PUBLIC_DOMAIN',
  None = 'NONE',
  Sample = 'SAMPLE',
}

export enum Country {
  De = 'DE',
}

export interface Epub {
  isAvailable?: boolean;
  acsTokenLink?: string;
  downloadLink?: string;
}

export enum TextToSpeechPermission {
  Allowed = 'ALLOWED',
}

export enum Viewability {
  AllPages = 'ALL_PAGES',
  NoPages = 'NO_PAGES',
  Partial = 'PARTIAL',
}

export enum Kind {
  BooksVolume = 'books#volume',
}

export interface SaleInfo {
  country?: Country;
  saleability?: Saleability;
  isEbook?: boolean;
  listPrice?: SaleInfoListPrice;
  retailPrice?: SaleInfoListPrice;
  buyLink?: string;
  offers?: Offer[];
}

export interface SaleInfoListPrice {
  amount?: number;
  currencyCode?: string;
}

export interface Offer {
  finskyOfferType?: number;
  listPrice?: OfferListPrice;
  retailPrice?: OfferListPrice;
  giftable?: boolean;
}

export interface OfferListPrice {
  amountInMicros?: number;
  currencyCode?: string;
}

export enum Saleability {
  ForSale = 'FOR_SALE',
  Free = 'FREE',
  NotForSale = 'NOT_FOR_SALE',
}

export interface SearchInfo {
  textSnippet?: string;
}

export interface VolumeInfo {
  title?: string;
  authors?: string[];
  publisher?: string;
  publishedDate?: string;
  description?: string;
  industryIdentifiers?: IndustryIdentifier[];
  readingModes?: ReadingModes;
  pageCount?: number;
  printType?: PrintType;
  categories?: string[];
  maturityRating?: MaturityRating;
  allowAnonLogging?: boolean;
  contentVersion?: string;
  panelizationSummary?: PanelizationSummary;
  imageLinks?: ImageLinks;
  language?: Language;
  previewLink?: string;
  infoLink?: string;
  canonicalVolumeLink?: string;
  subtitle?: string;
}

export interface ImageLinks {
  smallThumbnail?: string;
  thumbnail?: string;
}

export interface IndustryIdentifier {
  type?: Type;
  identifier?: string;
}

export enum Type {
  Isbn10 = 'ISBN_10',
  Isbn13 = 'ISBN_13',
  Other = 'OTHER',
}

export enum Language {
  De = 'de',
  En = 'en',
}

export enum MaturityRating {
  NotMature = 'NOT_MATURE',
}

export interface PanelizationSummary {
  containsEpubBubbles?: boolean;
  containsImageBubbles?: boolean;
}

export enum PrintType {
  Book = 'BOOK',
}

export interface ReadingModes {
  text?: boolean;
  image?: boolean;
}

// Converts JSON strings to/from your types
// and asserts the results of JSON.parse at runtime
export class Convert {
  public static toWelcome(json: string): IBooks {
    return cast(JSON.parse(json), r('Welcome'));
  }

  public static welcomeToJson(value: IBooks): string {
    return JSON.stringify(uncast(value, r('Welcome')), null, 2);
  }
}

function invalidValue(typ: any, val: any, key: any, parent: any = ''): never {
  const prettyTyp = prettyTypeName(typ);
  const parentText = parent ? ` on ${parent}` : '';
  const keyText = key ? ` for key "${key}"` : '';
  throw Error(`Invalid value${keyText}${parentText}. Expected ${prettyTyp} but got ${JSON.stringify(val)}`);
}

function prettyTypeName(typ: any): string {
  if (Array.isArray(typ)) {
    if (typ.length === 2 && typ[0] === undefined) {
      return `an optional ${prettyTypeName(typ[1])}`;
    } else {
      return `one of [${typ
        .map((a) => {
          return prettyTypeName(a);
        })
        .join(', ')}]`;
    }
  } else if (typeof typ === 'object' && typ.literal !== undefined) {
    return typ.literal;
  } else {
    return typeof typ;
  }
}

function jsonToJSProps(typ: any): any {
  if (typ.jsonToJS === undefined) {
    const map: any = {};
    typ.props.forEach((p: any) => (map[p.json] = { key: p.js, typ: p.typ }));
    typ.jsonToJS = map;
  }
  return typ.jsonToJS;
}

function jsToJSONProps(typ: any): any {
  if (typ.jsToJSON === undefined) {
    const map: any = {};
    typ.props.forEach((p: any) => (map[p.js] = { key: p.json, typ: p.typ }));
    typ.jsToJSON = map;
  }
  return typ.jsToJSON;
}

function transform(val: any, typ: any, getProps: any, key: any = '', parent: any = ''): any {
  function transformPrimitive(typ: string, val: any): any {
    if (typeof typ === typeof val) return val;
    return invalidValue(typ, val, key, parent);
  }

  function transformUnion(typs: any[], val: any): any {
    // val must validate against one typ in typs
    const l = typs.length;
    for (let i = 0; i < l; i++) {
      const typ = typs[i];
      try {
        return transform(val, typ, getProps);
      } catch (_) {}
    }
    return invalidValue(typs, val, key, parent);
  }

  function transformEnum(cases: string[], val: any): any {
    if (cases.indexOf(val) !== -1) return val;
    return invalidValue(
      cases.map((a) => {
        return l(a);
      }),
      val,
      key,
      parent
    );
  }

  function transformArray(typ: any, val: any): any {
    // val must be an array with no invalid elements
    if (!Array.isArray(val)) return invalidValue(l('array'), val, key, parent);
    return val.map((el) => transform(el, typ, getProps));
  }

  function transformDate(val: any): any {
    if (val === null) {
      return null;
    }
    const d = new Date(val);
    if (isNaN(d.valueOf())) {
      return invalidValue(l('Date'), val, key, parent);
    }
    return d;
  }

  function transformObject(props: { [k: string]: any }, additional: any, val: any): any {
    if (val === null || typeof val !== 'object' || Array.isArray(val)) {
      return invalidValue(l(ref || 'object'), val, key, parent);
    }
    const result: any = {};
    Object.getOwnPropertyNames(props).forEach((key) => {
      const prop = props[key];
      const v = Object.prototype.hasOwnProperty.call(val, key) ? val[key] : undefined;
      result[prop.key] = transform(v, prop.typ, getProps, key, ref);
    });
    Object.getOwnPropertyNames(val).forEach((key) => {
      if (!Object.prototype.hasOwnProperty.call(props, key)) {
        result[key] = transform(val[key], additional, getProps, key, ref);
      }
    });
    return result;
  }

  if (typ === 'any') return val;
  if (typ === null) {
    if (val === null) return val;
    return invalidValue(typ, val, key, parent);
  }
  if (typ === false) return invalidValue(typ, val, key, parent);
  let ref: any = undefined;
  while (typeof typ === 'object' && typ.ref !== undefined) {
    ref = typ.ref;
    typ = typeMap[typ.ref];
  }
  if (Array.isArray(typ)) return transformEnum(typ, val);
  if (typeof typ === 'object') {
    return typ.hasOwnProperty('unionMembers')
      ? transformUnion(typ.unionMembers, val)
      : typ.hasOwnProperty('arrayItems')
      ? transformArray(typ.arrayItems, val)
      : typ.hasOwnProperty('props')
      ? transformObject(getProps(typ), typ.additional, val)
      : invalidValue(typ, val, key, parent);
  }
  // Numbers can be parsed by Date but shouldn't be.
  if (typ === Date && typeof val !== 'number') return transformDate(val);
  return transformPrimitive(typ, val);
}

function cast<T>(val: any, typ: any): T {
  return transform(val, typ, jsonToJSProps);
}

function uncast<T>(val: T, typ: any): any {
  return transform(val, typ, jsToJSONProps);
}

function l(typ: any) {
  return { literal: typ };
}

function a(typ: any) {
  return { arrayItems: typ };
}

function u(...typs: any[]) {
  return { unionMembers: typs };
}

function o(props: any[], additional: any) {
  return { props, additional };
}

function m(additional: any) {
  return { props: [], additional };
}

function r(name: string) {
  return { ref: name };
}

const typeMap: any = {
  Welcome: o(
    [
      { json: 'kind', js: 'kind', typ: u(undefined, '') },
      { json: 'totalItems', js: 'totalItems', typ: u(undefined, 0) },
      { json: 'items', js: 'items', typ: u(undefined, a(r('Item'))) },
    ],
    false
  ),
  Item: o(
    [
      { json: 'kind', js: 'kind', typ: u(undefined, r('Kind')) },
      { json: 'id', js: 'id', typ: u(undefined, '') },
      { json: 'etag', js: 'etag', typ: u(undefined, '') },
      { json: 'selfLink', js: 'selfLink', typ: u(undefined, '') },
      { json: 'volumeInfo', js: 'volumeInfo', typ: u(undefined, r('VolumeInfo')) },
      { json: 'saleInfo', js: 'saleInfo', typ: u(undefined, r('SaleInfo')) },
      { json: 'accessInfo', js: 'accessInfo', typ: u(undefined, r('AccessInfo')) },
      { json: 'searchInfo', js: 'searchInfo', typ: u(undefined, r('SearchInfo')) },
    ],
    false
  ),
  AccessInfo: o(
    [
      { json: 'country', js: 'country', typ: u(undefined, r('Country')) },
      { json: 'viewability', js: 'viewability', typ: u(undefined, r('Viewability')) },
      { json: 'embeddable', js: 'embeddable', typ: u(undefined, true) },
      { json: 'publicDomain', js: 'publicDomain', typ: u(undefined, true) },
      { json: 'textToSpeechPermission', js: 'textToSpeechPermission', typ: u(undefined, r('TextToSpeechPermission')) },
      { json: 'epub', js: 'epub', typ: u(undefined, r('Epub')) },
      { json: 'pdf', js: 'pdf', typ: u(undefined, r('Epub')) },
      { json: 'webReaderLink', js: 'webReaderLink', typ: u(undefined, '') },
      { json: 'accessViewStatus', js: 'accessViewStatus', typ: u(undefined, r('AccessViewStatus')) },
      { json: 'quoteSharingAllowed', js: 'quoteSharingAllowed', typ: u(undefined, true) },
    ],
    false
  ),
  Epub: o(
    [
      { json: 'isAvailable', js: 'isAvailable', typ: u(undefined, true) },
      { json: 'acsTokenLink', js: 'acsTokenLink', typ: u(undefined, '') },
      { json: 'downloadLink', js: 'downloadLink', typ: u(undefined, '') },
    ],
    false
  ),
  SaleInfo: o(
    [
      { json: 'country', js: 'country', typ: u(undefined, r('Country')) },
      { json: 'saleability', js: 'saleability', typ: u(undefined, r('Saleability')) },
      { json: 'isEbook', js: 'isEbook', typ: u(undefined, true) },
      { json: 'listPrice', js: 'listPrice', typ: u(undefined, r('SaleInfoListPrice')) },
      { json: 'retailPrice', js: 'retailPrice', typ: u(undefined, r('SaleInfoListPrice')) },
      { json: 'buyLink', js: 'buyLink', typ: u(undefined, '') },
      { json: 'offers', js: 'offers', typ: u(undefined, a(r('Offer'))) },
    ],
    false
  ),
  SaleInfoListPrice: o(
    [
      { json: 'amount', js: 'amount', typ: u(undefined, 3.14) },
      { json: 'currencyCode', js: 'currencyCode', typ: u(undefined, '') },
    ],
    false
  ),
  Offer: o(
    [
      { json: 'finskyOfferType', js: 'finskyOfferType', typ: u(undefined, 0) },
      { json: 'listPrice', js: 'listPrice', typ: u(undefined, r('OfferListPrice')) },
      { json: 'retailPrice', js: 'retailPrice', typ: u(undefined, r('OfferListPrice')) },
      { json: 'giftable', js: 'giftable', typ: u(undefined, true) },
    ],
    false
  ),
  OfferListPrice: o(
    [
      { json: 'amountInMicros', js: 'amountInMicros', typ: u(undefined, 0) },
      { json: 'currencyCode', js: 'currencyCode', typ: u(undefined, '') },
    ],
    false
  ),
  SearchInfo: o([{ json: 'textSnippet', js: 'textSnippet', typ: u(undefined, '') }], false),
  VolumeInfo: o(
    [
      { json: 'title', js: 'title', typ: u(undefined, '') },
      { json: 'authors', js: 'authors', typ: u(undefined, a('')) },
      { json: 'publisher', js: 'publisher', typ: u(undefined, '') },
      { json: 'publishedDate', js: 'publishedDate', typ: u(undefined, '') },
      { json: 'description', js: 'description', typ: u(undefined, '') },
      { json: 'industryIdentifiers', js: 'industryIdentifiers', typ: u(undefined, a(r('IndustryIdentifier'))) },
      { json: 'readingModes', js: 'readingModes', typ: u(undefined, r('ReadingModes')) },
      { json: 'pageCount', js: 'pageCount', typ: u(undefined, 0) },
      { json: 'printType', js: 'printType', typ: u(undefined, r('PrintType')) },
      { json: 'categories', js: 'categories', typ: u(undefined, a('')) },
      { json: 'maturityRating', js: 'maturityRating', typ: u(undefined, r('MaturityRating')) },
      { json: 'allowAnonLogging', js: 'allowAnonLogging', typ: u(undefined, true) },
      { json: 'contentVersion', js: 'contentVersion', typ: u(undefined, '') },
      { json: 'panelizationSummary', js: 'panelizationSummary', typ: u(undefined, r('PanelizationSummary')) },
      { json: 'imageLinks', js: 'imageLinks', typ: u(undefined, r('ImageLinks')) },
      { json: 'language', js: 'language', typ: u(undefined, r('Language')) },
      { json: 'previewLink', js: 'previewLink', typ: u(undefined, '') },
      { json: 'infoLink', js: 'infoLink', typ: u(undefined, '') },
      { json: 'canonicalVolumeLink', js: 'canonicalVolumeLink', typ: u(undefined, '') },
      { json: 'subtitle', js: 'subtitle', typ: u(undefined, '') },
    ],
    false
  ),
  ImageLinks: o(
    [
      { json: 'smallThumbnail', js: 'smallThumbnail', typ: u(undefined, '') },
      { json: 'thumbnail', js: 'thumbnail', typ: u(undefined, '') },
    ],
    false
  ),
  IndustryIdentifier: o(
    [
      { json: 'type', js: 'type', typ: u(undefined, r('Type')) },
      { json: 'identifier', js: 'identifier', typ: u(undefined, '') },
    ],
    false
  ),
  PanelizationSummary: o(
    [
      { json: 'containsEpubBubbles', js: 'containsEpubBubbles', typ: u(undefined, true) },
      { json: 'containsImageBubbles', js: 'containsImageBubbles', typ: u(undefined, true) },
    ],
    false
  ),
  ReadingModes: o(
    [
      { json: 'text', js: 'text', typ: u(undefined, true) },
      { json: 'image', js: 'image', typ: u(undefined, true) },
    ],
    false
  ),
  AccessViewStatus: ['FULL_PUBLIC_DOMAIN', 'NONE', 'SAMPLE'],
  Country: ['DE'],
  TextToSpeechPermission: ['ALLOWED'],
  Viewability: ['ALL_PAGES', 'NO_PAGES', 'PARTIAL'],
  Kind: ['books#volume'],
  Saleability: ['FOR_SALE', 'FREE', 'NOT_FOR_SALE'],
  Type: ['ISBN_10', 'ISBN_13', 'OTHER'],
  Language: ['de', 'en'],
  MaturityRating: ['NOT_MATURE'],
  PrintType: ['BOOK'],
};