module.exports = {
    fromVolumeToMass(volumeInL,density){
        return volumeInL * density;
    },

    fromMolToMass(mol, molarMass){
        return mol * molarMass;
    }
}