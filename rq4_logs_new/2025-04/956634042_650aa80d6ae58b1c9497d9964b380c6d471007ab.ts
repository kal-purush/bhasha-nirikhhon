export const validateEmail = (value: string): string => {
  const regex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!value) return "El correo es requerido.";
  if (!regex.test(value)) return "Por favor, introduce un correo electrónico válido.";
  return "";
};

export const validatePassword = (value: string): string => {
  if (!value) return "La contraseña es requerida.";
  if (value.length < 8) return "La contraseña debe tener al menos 8 caracteres.";
  return "";
};

export const validateRepeatPassword = (value: string, original: string): string => {
  if (!value) return "Por favor, repite la contraseña.";
  if (value !== original) return "Las contraseñas no coinciden.";
  return "";
};

export const getPasswordStrength = (value: string): "Débil" | "Media" | "Fuerte" | "" => {
  if (!value) return "";
  const hasLetters = /[a-zA-Z]/.test(value);
  const hasNumbers = /[0-9]/.test(value);
  const hasSymbols = /[^a-zA-Z0-9]/.test(value);
  if (value.length < 8) return "Débil";
  if (hasLetters && hasNumbers && value.length >= 12 && hasSymbols) return "Fuerte";
  if (hasLetters && hasNumbers) return "Media";
  return "Débil";
};