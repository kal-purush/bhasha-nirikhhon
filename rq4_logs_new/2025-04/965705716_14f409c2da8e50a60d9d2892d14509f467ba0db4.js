let ingredients = [];

function addIngredient() {
    const input = document.getElementById('ingredientInput');
    const ingredient = input.value.trim();

    if (ingredient) {
        ingredients.push(ingredient);
        input.value = '';
        renderIngredients();
    }
}

function removeIngredient(index) {
    ingredients.splice(index, 1);
    renderIngredients();
}

function renderIngredients() {
    const ingredientList = document.getElementById('ingredientList');
    ingredientList.innerHTML = '';

    ingredients.forEach((ingredient, index) => {
        const div = document.createElement('div');
        div.innerHTML = `
            <span>${ingredient}</span>
            <button onclick="removeIngredient(${index})">Eliminar</button>
        `;
        ingredientList.appendChild(div);
    });
}

async function generateRecipe() {
    const recipeOutput = document.getElementById('recipeOutput');
    const vegetariano = document.getElementById('vegetariano').checked;
    const vegano = document.getElementById('vegano').checked;
    const sinGluten = document.getElementById('sinGluten').checked;
    const sinLacteos = document.getElementById('sinLacteos').checked;
    const sinNueces = document.getElementById('sinNueces').checked;

    if (ingredients.length === 0) {
        recipeOutput.innerHTML = '<p>Por favor, añade al menos un ingrediente.</p>';
        return;
    }

    // Construir las restricciones dietéticas
    let restrictions = [];
    if (vegetariano) restrictions.push('vegetariano');
    if (vegano) restrictions.push('vegano');
    if (sinGluten) restrictions.push('sin gluten');
    if (sinLacteos) restrictions.push('sin lácteos');
    if (sinNueces) restrictions.push('sin nueces');

    // Crear el prompt para la IA
    const prompt = `
        Genera una receta de cocina española usando los siguientes ingredientes: ${ingredients.join(', ')}.
        ${restrictions.length > 0 ? `La receta debe ser ${restrictions.join(' y ')}.` : ''}
        Proporciona el nombre de la receta, el tiempo de preparación, una lista de ingredientes (incluyendo los proporcionados y otros necesarios), y las instrucciones paso a paso.
        Asegúrate de que el formato sea claro y organizado.
    `;

    // Mostrar un mensaje de carga mientras la IA genera la receta
    recipeOutput.innerHTML = '<p>Generando receta, por favor espera...</p>';

    try {
        // Configurar la solicitud a la API de Hugging Face
        const API_KEY = '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'; // Token proporcionado
        const response = await fetch('https://api-inference.huggingface.co/models/mixtral-8x7b-instruct-v0.1', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${API_KEY}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                inputs: prompt,
                parameters: {
                    max_length: 500,
                    temperature: 0.7,
                    top_p: 0.9,
                },
            }),
        });

        const data = await response.json();

        if (data && data[0] && data[0].generated_text) {
            // Extraer la respuesta generada
            const generatedRecipe = data[0].generated_text.trim();

            // Formatear la respuesta para que se vea bien en HTML
            const formattedRecipe = generatedRecipe
                .replace(/\n\n/g, '</p><p>')
                .replace(/\n/g, '<br>')
                .replace(/### (.*?)(<br>|<\/p>)/g, '<h3>$1</h3>')
                .replace(/\*\*Ingredientes:\*\*/g, '<h4>Ingredientes:</h4><ul>')
                .replace(/\*\*Instrucciones:\*\*/g, '</ul><h4>Instrucciones:</h4><ol>')
                .replace(/- (.*?)(<br>|<\/p>)/g, '<li>$1</li>')
                .replace(/\d+\. (.*?)(<br>|<\/p>)/g, '<li>$1</li>')
                .replace(/<\/ul>/g, '</ul>')
                .replace(/<\/ol>/g, '</ol>');

            recipeOutput.innerHTML = `<p>${formattedRecipe}</p>`;
        } else {
            recipeOutput.innerHTML = '<p>Lo siento, no se pudo generar la receta. Intenta de nuevo.</p>';
        }
    } catch (error) {
        console.error('Error al generar la receta:', error);
        recipeOutput.innerHTML = '<p>Hubo un error al generar la receta. Por favor, intenta de nuevo más tarde.</p>';
    }
}