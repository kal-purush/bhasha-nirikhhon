type dataType = {
    title?: string;
    phone?: number;
    appointmentPrice?: number;
    description?: string;
  };
  
  type ValidationErrors = Partial<Record<keyof dataType | "profilePicture", string>>;
  
  const providerFormValidatorsUpdate = (
    data: dataType,
    profilePicture: File | null
  ): ValidationErrors => {
    const errors: ValidationErrors = {};
  
    // Validación para `title`
    if (!data.title || data.title.length < 8 || /\d/.test(data.title)) {
      errors.title = "El título debe tener al menos 8 caracteres y no contener números.";
    }
  
    // Validación para `phone`
    if (
      data.phone === undefined ||
      data.phone < 1_000_000 || // mínimo 7 dígitos
      data.phone > 999_999_999_999_999 // máximo 15 dígitos
    ) {
      errors.phone = "El teléfono debe ser un número válido entre 7 y 15 dígitos.";
    }
  
    // Validación para `appointmentPrice`
    if (
      data.appointmentPrice === undefined ||
      data.appointmentPrice <= 0 ||
      !Number.isInteger(data.appointmentPrice)
    ) {
      errors.appointmentPrice = "El precio debe ser un número entero mayor a 0.";
    }
  
    // Validación para `description`
    if (!data.description || data.description.length <= 20) {
      errors.description = "La descripción debe tener más de 20 caracteres.";
    }

  
    // Validación para `profilePicture`
    if (!(profilePicture instanceof File)) {
      errors.profilePicture = "Debes subir una imagen de perfil válida. Solo formatos .webp .jpeg y .png.";
    }
  
    return errors;
  };
  
  export default providerFormValidatorsUpdate;
  