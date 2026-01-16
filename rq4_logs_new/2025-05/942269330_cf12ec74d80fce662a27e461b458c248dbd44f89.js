import Category from '../models/Category.js';

// ➕ Ajouter une catégorie
export const addCategory = async (req, res) => {
  const { name, level } = req.body;
  try {
    // Vérifier si elle existe déjà
    const existing = await Category.findOne({ name, level });
    if (existing) {
      return res.status(400).json({ msg: 'Catégorie déjà existante pour ce niveau' });
    }

    // Sinon, la créer
    const newCategory = new Category({ name, level });
    await newCategory.save();

    // Populer le niveau dans la catégorie avant de renvoyer la réponse
    const populatedCategory = await Category.findById(newCategory._id).populate('level');

    res.status(201).json(populatedCategory);  // Retourne la catégorie avec le niveau peuplé
  } catch (err) {
    console.error(err);
    res.status(500).json({ msg: 'Erreur serveur' });
  }
};
export const getCategoriesByLevel = async (req, res) => {
  try {
    const { levelId } = req.params;

    // Si levelId est présent, filtrer par niveau
    let categories;
    if (levelId) {
      categories = await Category.find({ level: levelId }).populate('level');
    } else {
      // Sinon, récupérer toutes les catégories
      categories = await Category.find().populate('level');
    }

    res.status(200).json(categories);
  } catch (err) {
    console.error('Erreur lors de la récupération des catégories:', err);
    res.status(500).json({ message: 'Erreur serveur' });
  }
};

export const deleteCategory = async (req, res) => {
  try {
    const cat = await Category.findByIdAndDelete(req.params.id);

    if (!cat) {
      return res.status(404).json({ msg: 'Catégorie non trouvée' });
    }

    // Retourner une réponse plus claire avec l'ID de la catégorie supprimée
    res.json({ msg: `Catégorie avec l'ID ${req.params.id} supprimée` });

  } catch (err) {
    console.error('Erreur de suppression:', err);
    res.status(500).json({ error: 'Erreur serveur lors de la suppression de la catégorie' });
  }
};


// Modifier
export const updateCategory = async (req, res) => {
  try {
    const { name } = req.body;
    const cat = await Category.findByIdAndUpdate(
      req.params.id,
      { name },
      { new: true }
    );
    if (!cat) return res.status(404).json({ msg: 'Catégorie non trouvée' });
    res.json(cat);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};