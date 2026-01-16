export const fetchCuriosities = async () => {
    const response = await fetch('/src/data/curiosities.json');
    if (!response.ok) {
        throw new Error('Failed to fetch data');
    }
    return await response.json();
};
const init = async () => {
    try {
        const curiositiesData = await fetchCuriosities();
        // Atribui os dados do JSON diretamente à variável curiosities
        let curiosities = curiositiesData.curiosities;
        // Agora 'curiosities' contém os dados do JSON
        console.log(curiosities);
    }
    catch (error) {
        console.error('Error fetching data:', error);
    }
};
// Chama a função para inicializar a lógica
init();