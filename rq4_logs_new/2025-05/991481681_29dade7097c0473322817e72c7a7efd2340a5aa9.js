// This script manages the changes on the form when the account type changes

document.addEventListener('DOMContentLoaded', function() {
    const accountTypeRadios = document.querySelectorAll('input[name="account_type"]');
    const full_name_label = document.getElementById("full_name_label");
    const contact_title = document.getElementById('contact_title');
    const company_name = document.getElementById('company_name');
    
    function updateFormFields() {
        const isCompanyAccount = document.querySelector('input[name="account_type"]:checked').value === 'company';
        full_name_label.textContent = isCompanyAccount ? 'Contact Name' : 'Full Name';
        contact_title.style.display = isCompanyAccount ? 'flex' : 'none';
        company_name.style.display = isCompanyAccount ? 'flex' : 'none';       
    }
        
    accountTypeRadios.forEach(radio => {
        radio.addEventListener('change', updateFormFields);
    });    
    
    updateFormFields();
});