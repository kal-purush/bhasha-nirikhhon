/**
 * Class representing a composition of cosmetic ingredients.
 */
class Compositions {
    /**
     * Create a composition.
     * @param {string} name - The name of the composition.
     * @param {Array<string>} ingredients - The ingredients of the composition.
     */
    constructor() {
        this.presets = [];
        this.setPresets();
        this.activePresetId = 1;
    }

    getPresets() {
        return this.presets;
    }

    setPresets() {
        const chemicalIngredients = [
            {
                label: 'Hyaluronic Acid',
                id: 1,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'intense hydration' },
                    { value: '+', label: 'plumping effects' },
                    { value: '+', label: 'smoothing fine lines' },
                    { value: '-', label: 'skin irritation' }
                ]
            },
            {
                label: 'Retinoids',
                id: 2,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'boost collagen production' },
                    { value: '+', label: 'accelerate cell turnover' },
                    { value: '+', label: 'reduce wrinkles and age spots' },
                    { value: '-', label: 'skin irritation' }
                ]
            },
            {
                label: 'Vitamin C',
                id: 3,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'brightens skin' },
                    { value: '+', label: 'antioxidant that protects against free radical damage' },
                    { value: '+', label: 'boosts collagen production' },
                    { value: '+', label: 'reducing signs of aging' },
                    { value: '-', label: 'skin irritation' }
                ]
            },
            {
                label: 'Peptides',
                id: 4,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'support collagen and elastin production' },
                    { value: '+', label: 'promoting firmer and smoother skin' },
                    { value: '+', label: 'reduce the appearance of wrinkles' }
                ]
            },
            {
                label: 'Niacinamide',
                id: 5,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'reduces inflammation' },
                    { value: '+', label: 'improves skin elasticity' },
                    { value: '+', label: 'fades hyperpigmentation' },
                    { value: '+', label: 'strengthens the skin barrier' }
                ]
            },
            {
                label: 'Ceramides',
                id: 6,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'helps restore youthful resilience' },
                    { value: '+', label: 'reduce fine lines' }
                ]
            },
            {
                label: 'AHAs',
                id: 7,
                groupId: 1,
                step: 5,
                effects: [
                    { value: '+', label: 'remove dead skin cells' },
                    { value: '+', label: 'improve skin texture' }
                ]
            },
            {
                label: 'Jojoba Oil',
                id: 8,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Squalane',
                id: 9,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Shea Butter',
                id: 10,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Rosehip Oil',
                id: 11,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Argan Oil',
                id: 12,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Grapeseed Oil',
                id: 13,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Avocado Oil',
                id: 14,
                groupId: 2,
                step: 5,
                effects: []
            },
            {
                label: 'Glycerin',
                id: 15,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Hyaluronic Acid',
                id: 16,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Aloe Vera',
                id: 17,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Ceramides',
                id: 18,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Sodium PCA',
                id: 19,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Butylene',
                id: 20,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Urea',
                id: 21,
                groupId: 3,
                step: 5,
                effects: []
            },
            {
                label: 'Cetearyl Alcohol',
                id: 22,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Glyceryl Stearate',
                id: 23,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Polysorbates',
                id: 24,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Cetearyl Glucoside',
                id: 25,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Lecithin',
                id: 26,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'PEG-100',
                id: 27,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Sorbitan Stearate',
                id: 28,
                groupId: 4,
                step: 5,
                effects: []
            },
            {
                label: 'Xanthan Gum',
                id: 29,
                groupId: 5,
                step: 25,
                effects: []
            },
            {
                label: 'Carbomers',
                id: 30,
                groupId: 5,
                step: 25,
                effects: []
            },
            {
                label: 'Hydroxyethylcellulose',
                id: 31,
                groupId: 5,
                step: 25,
                effects: []
            },
            {
                label: 'Cetearyl Alcohol',
                id: 32,
                groupId: 5,
                step: 25,
                effects: []
            },
            {
                label: 'Chelating Agents',
                id: 33,
                groupId: 6,
                step: 25,
                effects: []
            },
            {
                label: 'pH Adjusters',
                id: 34,
                groupId: 6,
                step: 25,
                effects: []
            },
            {
                label: 'Antioxidants',
                id: 35,
                groupId: 6,
                step: 25,
                effects: []
            }
        ];
        const presets = [
            {id: 1,
            name: 'Face cream',
            ingredients: ['Water', 'Retinoids', 'Vitamin C',
                'Rosehip Oil',
            'Aloe Vera',
                'Glyceryl Stearate',
                'Hydroxyethylcellulose',
                'pH Adjusters'
            ],
            ingredientsValues: [
                {ingredientId: 0, value: 70}, {ingredientId: 2, value: 5, inGroup: 50}, {ingredientId: 3, value: 5, inGroup: 50},
                {ingredientId: 11, value: 5, inGroup: 100},
                {ingredientId: 17, value: 5, inGroup: 100},
                {ingredientId: 23, value: 5, inGroup: 100},
                {ingredientId: 31, value: 5, inGroup: 100},
                {ingredientId: 34, value: 5, inGroup: 100}]
            },
            {id: 2,
            name: 'Body lotion',
            ingredients: ['Water', 'Hyaluronic Acid', 'Jojoba Oil', 'Shea Butter', 'Cetearyl Alcohol', 'Xanthan Gum', 'Antioxidants'],
            ingredientsValues: [
                {ingredientId: 0, value: 70}, {ingredientId: 15, value: 5, inGroup: 100}, {ingredientId: 8, value: 5, inGroup: 100},
                {ingredientId: 10, value: 5, inGroup: 100}, {ingredientId: 22, value: 5, inGroup: 100}, {ingredientId: 29, value: 5, inGroup: 100},
                {ingredientId: 35, value: 5, inGroup: 100}]
            },
        ];
        this.presets = presets;
    }

    getIngredientValueByIngredientId(ingredientId, groupPercent = false) {
        const issetIngredient = this.presets.find(preset => preset.id === this.activePresetId).ingredientsValues.find(ingredient => ingredient.ingredientId === ingredientId);
        if (issetIngredient) {
            return groupPercent ? issetIngredient.inGroup : issetIngredient.value;
        }
        return 0;
    }

    setActivePresetId(presetId) {
        this.activePresetId = presetId;
    }

    getActivePresetId() {
        return this.activePresetId;
    }

    setActivePresetId(presetId) {
        this.activePresetId = presetId;
    }

    activePreset() {
        return this.presets.find(preset => preset.id === this.activePresetId);
    }
}

export default Compositions;