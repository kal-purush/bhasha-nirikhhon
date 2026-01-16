import { style } from '@vanilla-extract/css';

export const ProblemTitleStyle = style({
  fontSize: '2.8rem',
});

const ProblemDiffFont = style({
  fontSize: '1.5rem',
  marginLeft: '30px',
});
export const ProblemDiffStyle = {
  easy: style([ProblemDiffFont, { color: '#7BA918' }]),
  medium: style([ProblemDiffFont, { color: '#D49228' }]),
  hard: style([ProblemDiffFont, { color: '#D74640' }]),
  extreme: style([ProblemDiffFont, { color: '#AC258C' }]),
};

export const ProblemCategoryStyle = style({
  fontSize: '1.2rem',
});

export const ProblemCodeBlockStyle = style({
  background: '#F5F5F5',
  padding: '30px 0',
  borderRadius: '10px',
});

export const ProblemCodeStyle = style({
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  fontSize: '1.0rem',
});