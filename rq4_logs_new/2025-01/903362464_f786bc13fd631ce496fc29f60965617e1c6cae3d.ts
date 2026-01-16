import { fontSizes, fontWeights } from '@/styles/fontStyles.css';
import { globalStyle, style } from '@vanilla-extract/css';
import { global } from '@/styles/global.css';

export const activitiesPageContainer = style({
  marginBottom: '12rem',
  maxWidth: '80rem',
  margin: '0 auto',
});

export const topLayout = style({
  display: 'flex',
  justifyContent: 'space-between',
  maxWidth: '80rem',
  margin: '0 auto',
  marginBottom: '3.4rem',
});

export const h2Title = style([fontSizes.text3xl, fontWeights.bold]);

export const createButton = style([
  {
    backgroundColor: global.color.nomadBlack,
    color: '#fff',
    padding: '0.8rem 1.6rem',
    borderRadius: '0.4rem',
  },
  fontSizes.textLg,
]);

export const selectBoxContainer = style({
  borderRadius: '0.4rem',
  position: 'relative',
});

export const errorStyle = style({
  border: `1px solid ${global.color.red[300]}`,
  selectors: {
    '&:focus-within': {
      border: `1px solid ${global.color.red[300]}`,
    },
  },
});

export const dashedErrorStyle = style({
  border: `1px dashed ${global.color.red[300]}`,
  selectors: {
    '&:focus-within': {
      border: `1px dashed ${global.color.red[300]}`,
    },
  },
});

export const errorMessageStyle = style({
  margin: '0.8rem 0 2.1rem',
  color: global.color.red[300],
});

export const inputTitle = style([{}, fontSizes.text2xl, fontWeights.bold]);

export const subImageUploadBox = style({
  border: `1px dashed ${global.color.gray[900]}`,
  borderRadius: '1.2rem',
  width: '18rem',
  height: '18rem',
  cursor: 'pointer',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
});

export const subImageContainer = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: '2.4rem',
});

export const fileUploadLayout = style({
  display: 'flex',
  gap: '1.6rem',
});

export const previewImageContainer = style({
  position: 'relative',
});

export const previewImageBox = style({
  borderRadius: '2.4rem',
  width: '18rem',
  height: '18rem',
  overflow: 'hidden',
  position: 'relative',
});

export const btnCanceled = style({
  position: 'absolute',
  right: '-1.2rem',
  top: '-1.6rem',
  cursor: 'pointer',
});

export const fileUploadContainer = style({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  flexDirection: 'column',
  borderRadius: '1.2rem',
  width: '18rem',
  height: '18rem',
  cursor: 'pointer',
});

export const fileUploadDefault = style([fileUploadContainer, { border: `1px dashed ${global.color.gray[900]}` }]);

export const fileUploadWithError = style([fileUploadContainer, dashedErrorStyle]);

export const fileUploadtext = style([
  {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    gap: '3rem',
    color: global.color.gray[900],
  },
  fontSizes.text2xl,
]);

export const fileUploadInput = style({
  display: 'none',
});

export const arrow = style({
  transition: 'transform 0.3s',
});

export const rotated = style({
  transform: 'rotate(180deg)',
});

export const descriptionInput = style({
  resize: 'none',
  width: '100%',
  height: '34.6rem',
  borderRadius: '0.4rem',
  padding: '1.6rem 0.8rem 0 1.6rem',

  '::placeholder': {
    color: global.color.gray[600],
  },
});

export const inputContainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '1.6rem',
});

export const inputSectionContainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '2.4rem',
  marginBottom: '42.9rem',
});

export const waveSign = style({
  margin: '0 1.2rem',
});

export const datePickerLabel = style([
  {
    display: 'block',
    marginBottom: '8px',
    fontSize: '2rem',
  },
  fontWeights.medium,
  fontSizes.textXl,
]);

export const categoryLayout = style({
  cursor: 'pointer',
});

export const categoryInput = style({
  width: '100%',
  height: '100%',
  cursor: 'pointer',
  border: 'none',

  selectors: {
    '&::placeholder': {
      color: global.color.gray[600],
      fontWeight: '400',
    },
    '&:focus': {
      outline: 'none',
    },
  },
});

export const selectBoxList = style({
  position: 'absolute',
  top: '6.4rem',
  width: '100%',
  background: '#fff',
  borderRadius: '0.4rem',
  zIndex: 1,
  boxShadow: '0 10px 30px 3px rgba(5, 16, 55, 0.15)',
  padding: '0.8rem 0',
});

export const selectBoxItem = style({
  padding: '0.8rem',
  margin: '0.8rem',
  borderRadius: '0.6rem',
  cursor: 'pointer',
  display: 'flex',
  alignItems: 'center',
  gap: '0.8rem',

  ':hover': {
    background: global.color.nomadBlack,
    color: '#fff',
  },
});

export const selectedOptionStyle = style({
  color: global.color.black,
  display: 'flex',
  height: '5.4rem',
  alignItems: 'center',
  borderRadius: '0.4rem',
  overflow: 'hidden',
  padding: '1.5rem 1.6rem',
  cursor: 'pointer',

  selectors: {
    '&:focus-within': {
      border: `1px solid ${global.color.green[200]}`,
    },
  },
});

export const defaultInputStyle = style({
  border: `1px solid ${global.color.gray[800]}`,
});

export const checkMark = style({
  visibility: 'hidden',
  selectors: {
    [`${selectBoxItem}:hover &`]: {
      visibility: 'visible',
    },
  },
});

// 달력 커스텀
globalStyle('.react-datepicker__input-container .react-datepicker__calendar-icon', {
  right: '1.6rem',
});

globalStyle('.react-datepicker__view-calendar-icon input', {
  padding: '8px 0 8px 16px',
});

globalStyle('.react-datepicker__input-container', {
  position: 'relative',
  display: 'flex',
  alignItems: 'center',
  width: '100%',
  height: '40px',
});

export const datePickerLabelContainer = style({
  display: 'grid',
  // 793px 기준
  // gridTemplateColumns: '399px 178px 216px'
  gridTemplateColumns: '50.31% 22.44% 27.23%',
});

export const horizon = style({
  width: '100%',
  border: `1px solid ${global.color.gray[300]}`,
  margin: '2.1rem 0',
});

export const dateTimePickerContainer = style({
  display: 'grid',
  gridTemplateColumns: '1fr 1fr auto',
  gap: '2rem',
  height: '5.6rem',
  alignItems: 'center',
});

export const TimeButton = style({
  cursor: 'pointer',
});

export const datePickerContainer = style({
  width: '37.9rem',
});

export const timePickerContainer = style({
  display: 'flex',
  gap: '1.2rem',
  alignItems: 'center',
});

export const descPhrase = style([
  {
    color: global.color.gray[900],
  },
  fontSizes.text2lg,
]);

globalStyle('.css-jupps9-MuiInputBase-root-MuiOutlinedInput-root', {
  height: '5.6rem',
});

// mui input placeholder 글씨 크기
globalStyle('.css-1dune0f-MuiInputBase-input-MuiOutlinedInput-input', {
  fontSize: '1.6rem',
});

globalStyle('.css-17f9e7e-MuiTypography-root-MuiDayCalendar-weekDayLabel', {
  fontSize: '1.4rem',
});

globalStyle('.css-4k4mmf-MuiButtonBase-root-MuiPickersDay-root', {
  fontSize: '1.3rem',
});

globalStyle('.css-qct7wd-MuiButtonBase-root-MuiPickersDay-root:focus.Mui-selected', {
  fontSize: '1.3rem',
});

globalStyle('.css-qct7wd-MuiButtonBase-root-MuiPickersDay-root:focus', {
  fontSize: '1.3rem',
});

globalStyle('.css-1chuxo2-MuiPickersCalendarHeader-label', {
  fontSize: '1.4rem',
});

// 시계 글씨 크기
globalStyle('.css-ux17pc-MuiButtonBase-root-MuiMenuItem-root-MuiMultiSectionDigitalClockSection-item', {
  fontSize: '1.3rem',
});

// 시계 ok 글씨 크기
globalStyle('.css-1588512-MuiButtonBase-root-MuiButton-root', {
  fontSize: '1.4rem',
});

// input 내, svg 크기
globalStyle('.css-1umw9bq-MuiSvgIcon-root', {
  width: '32px',
  height: '32px',
});