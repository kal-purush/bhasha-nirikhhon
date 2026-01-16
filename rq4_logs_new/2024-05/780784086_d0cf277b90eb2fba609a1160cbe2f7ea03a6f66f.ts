export const RelativeTimeFormatter = new Intl.RelativeTimeFormat('en', { style: 'long' });
export const USTimeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: 'numeric',
  hour12: false,
  minute: 'numeric',
  timeZone: 'America/Chicago',
});

export const discordId = '196399908771725312';

export const dateOfBirth = new Date('2000-12-20');

export const age = new Date(Date.now() - dateOfBirth.getTime()).getUTCFullYear() - 1970;

export const hasBirthdayPassed =
  new Date().getMonth() >= dateOfBirth.getMonth() && new Date().getDate() >= dateOfBirth.getDate();

export const nextBirthdayYear = new Date().getFullYear() + (hasBirthdayPassed ? 1 : 0);

export const daysUntilBirthday = RelativeTimeFormatter.formatToParts(
  Math.floor(
    (new Date(nextBirthdayYear, dateOfBirth.getMonth(), dateOfBirth.getDay() + 1).getTime() - Date.now()) /
      1000 /
      60 /
      60 /
      24
  ),
  'day'
)[1]!.value.toString();