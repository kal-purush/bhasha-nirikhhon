export type FontSize = '14' | '16';

export type ButtonVariant = keyof typeof buttonVariant;

export type ButtonSize = keyof typeof buttonSize;

export const buttonSize = {
  fullWidth: 'w-full h-[47px] rounded-xl',
  xs: 'w-[74px] h-8 rounded-xl',
  sm: 'w-[111px] h-10 rounded-[40px]',
  md: 'w-[125px] h-12 rounded-[40px]',
  lg: 'w-[138px] h-10 rounded-[40px]',
  xl: 'w-[373px] h-12 rounded-[40px]',
};

export const buttonVariant = {
  solid: 'bg-primary text-white',
  'outline-primary': 'bg-white text-primary text-primary border border-primary',
  'outline-gray': 'bg-white text-gray500 text-primary border border-gray300',
  'ghost-primary': 'bg-transparent text-primary border border-primary',
  'ghost-white': 'bg-transparent text-white border border-white',
  danger: 'bg-danger text-white',
  gradient: 'bg-gradient text-white',
} as const;

export const disabledButton: Partial<Record<keyof typeof buttonVariant, string>> = {
  solid: 'bg-gray400 text-white',
  'outline-primary': 'border border-gray400 text-gray400',
  'ghost-primary': 'border border-gray400 text-gray400',
  'ghost-white': 'border border-gray400 text-gray400',
};

export const buttonFontSize: Record<FontSize, string> = {
  '14': 'text-md-semi',
  '16': 'text-lg-semi',
};