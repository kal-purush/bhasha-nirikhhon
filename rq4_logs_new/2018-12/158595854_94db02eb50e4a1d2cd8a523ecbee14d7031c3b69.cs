using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

/*
    GlazeChemistry.cs

    The purpose of this .cs is to contain all items for glaze calculation that are directly chemistry related in one concise area.
    The properties allotted to each item will be expanded as need arises.
    The JSON files linked here are intended to be PeriodicTableElements.json, SimpleMolecules.json, and CompoundMolecules.json.
    This .cs was not intended to contain the materials or other glaze specific properties. Only the root elements and the molecules they create.
*/

namespace GlazeChemistry
{
    // GlazeChemistry manages all the purely chemical functionality of the GlazeBuilder namespace.
    // It controls the elements, molecules, and atomic/molecular weights of each component piece.
    class ChemicalDatabase
    {
        // Constructor
        // Requires, in order, a file for the elements, a file for the simple molecules, and a file for the compound molecules, all in JSON.
        public ChemicalDatabase(string elements_filename, string simple_molecules_filename, string compound_molecules_filename)
        {
            // Extracts the text for the elements into a string then parses that string into a JSON object.
            string elements_file = System.IO.File.ReadAllText(elements_filename);
            JObject elements_full_json = JObject.Parse(elements_file);

            // Populates the Periodic Table of Elements using the elements JSON object.
            PeriodicTable = new PeriodicTable(elements_full_json);
             
            // Extracts the text for the simple molecules into a string then parses that string into a JSON object.
            string simple_molecules_file = System.IO.File.ReadAllText(simple_molecules_filename);
            JObject simple_molecules_json = JObject.Parse(simple_molecules_file);

            // Extracts the text for the compound molecules into a string then parses that string into a JSON object.
            string compound_molecules_file = System.IO.File.ReadAllText(compound_molecules_filename);
            JObject compound_molecules_json = JObject.Parse(compound_molecules_file);

            // Populates the molecular dictionary with the simple molecule JSON, the compound molecule JSON, and the Periodic Table of Elements.
            MolecularDictionary = new MolecularDictionary(simple_molecules_json, compound_molecules_json, PeriodicTable);
        }

        // Properties
        // Declaration for the Periodic Table of Elements and Molecular Dictionary properties inherent to the GlazeChemistry class.
        PeriodicTable PeriodicTable { get; set; }
        MolecularDictionary MolecularDictionary { get; set; }

        // Function
        // Looks up any given molecule. Returns a generic container with the molecule in it.
        public Molecule LookupMolecule(string molecule_name)
        {
            if (MolecularDictionary.Contains(molecule_name))
            {
                return MolecularDictionary.Get(molecule_name);
            }
            else
            {
                return null;
            }
        }
    }
}