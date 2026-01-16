import { PersonaInfoData, ProfileData } from "../components/personal-info";
import { ResumeSchema } from "../types/resume";
import * as countries from 'i18n-iso-countries';
import * as en from 'i18n-iso-countries/langs/en.json';

countries.registerLocale(en);

export function extractAddress(resume: ResumeSchema): string {
  return `${resume.basics.location.city}, ${countries.getName(resume.basics.location.countryCode, 'en')}`;
}

export function extractProfiles(resume: ResumeSchema): ProfileData[] {
  return resume.basics.profiles.map(p => ({
    network: p.network,
    url: new URL(p.url),
    username: p.username
  }));
}

export function extractPersonalInfo(resume: ResumeSchema): PersonaInfoData {
  return {
    name: resume.basics.name,
    label: resume.basics.label,
    email: resume.basics.email,
    mobile: resume.basics.phone,
    telephone: '',
    address: extractAddress(resume),
    profiles: extractProfiles(resume)
  }
}