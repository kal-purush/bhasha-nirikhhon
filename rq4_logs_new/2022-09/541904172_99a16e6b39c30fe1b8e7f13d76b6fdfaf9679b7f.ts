const createFormData = (formValue: any) => {
    const formData = new FormData();

    const keys = Object.keys(formValue);
    keys.forEach((key) => {
        if (!formValue[key]) formValue[key] = "";
        
        if (key === "photo" && !!formValue.photo)
            formData.append("photo", formValue.photo.file);
        else if (key !== "photo") formData.set(key, formValue[key]);
    });

    return formData;
};

export default createFormData;