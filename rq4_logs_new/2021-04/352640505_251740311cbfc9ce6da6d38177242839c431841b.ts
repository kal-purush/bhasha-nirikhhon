export const PATTERNS = [
  '0,0.0000',
  '0,0',
  '+0,0',
  '0,0.0',
  '0.000',
  '00000',
  '000000,0',
  '000.00',
  '0[.]00000',
  '(0,0.0000)',
  '.00',
  '(.00)',
  '0.00000',
  '0.0[0000]',
  '0.0a',
  '0 a',
  '0a',
  '0o',
];

export const OPERATIONS = ['add', 'subtract', 'multiply', 'divide'];

export const DEFAULT_PATTERN = {
  label: 'widget_customization_format_value_settings.original_data',
  value: '0',
};

export const PATTERNS_OPTIONS = PATTERNS.map((pattern) => ({
  label: pattern,
  value: pattern,
}));
export const OPERATIONS_OPTIONS = OPERATIONS.map((operation) => ({
  label: operation.replace(/^\w/, (c: string) => c.toUpperCase()),
  value: operation,
}));