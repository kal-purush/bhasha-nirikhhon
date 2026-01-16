/**
 * Handles the multiselect field functionality in forms
 * Ensures proper value serialization for allowed_countries field
 */
document.addEventListener('DOMContentLoaded', function() {
    // Fix for enhanced UI multiselect fields in Gravity Forms
    try {
        const multiSelects = document.querySelectorAll('.gform-settings-field select[multiple]');
        
        multiSelects.forEach(function(select) {
            // Early return if element missing
            if (!select) return;
            
            const fieldName = select.getAttribute('name');
            if (!fieldName || typeof fieldName !== 'string') return;
            
            // Only process our target fields
            if (fieldName.indexOf('allowed_countries') !== -1) {
                const form = select.closest('form');
                if (!form) return;
                
                form.addEventListener('submit', function() {
                    try {
                        // Clean up previous inputs
                        document.querySelectorAll('input.multiselect-fix-value[data-for="' + fieldName + '"]')
                            .forEach(el => el.remove());
                        
                        // Get selected values
                        const selectedOptions = Array.from(select.selectedOptions);
                        const selectedValues = selectedOptions.map(option => option.value);
                        
                        // Create JSON safely
                        let jsonValue;
                        try {
                            jsonValue = JSON.stringify(selectedValues);
                        } catch (jsonError) {
                            jsonValue = '[]';
                            console.error('Error stringifying selected values:', jsonError);
                        }
                        
                        // Create hidden input
                        const hiddenInput = document.createElement('input');
                        hiddenInput.type = 'hidden';
                        hiddenInput.className = 'multiselect-fix-value';
                        hiddenInput.setAttribute('data-for', fieldName);
                        hiddenInput.name = '_gf_multiselect_values_' + fieldName;
                        hiddenInput.value = jsonValue;
                        form.appendChild(hiddenInput);
                    } catch (submitError) {
                        console.error('Error handling multiselect field:', submitError);
                    }
                });
            }
        });
    } catch (error) {
        console.error('Error in multiselect-fix.js:', error);
    }
});