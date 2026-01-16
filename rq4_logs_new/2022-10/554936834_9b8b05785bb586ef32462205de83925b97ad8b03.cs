using CT.CASE.Generator.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CT.CASE.Generator
{
    partial class BindingsTemplate
    {
        private readonly Schema Schema;
        private readonly IDictionary<Iri, Class> ClassesByIri;

        internal BindingsTemplate(Schema schema)
        {
            Schema = schema;
            ClassesByIri = Schema.Classes.ToDictionary((cls) => cls.Iri);
        }

        private IEnumerable<Field> AllFieldsOnClass(Class cls)
        {
            var fieldNamesOrder = new List<Iri>(); // field names (paths) in the order we first encountered them, starting with the most distant ancestor
            var fieldsByName = new Dictionary<Iri, Field>(); // fields on this class, keyed by each field's path
            var ancestors = GetAncestors(cls);
            ancestors.Reverse();
            foreach (var ancestor in ancestors)
            {
                foreach (var field in ancestor.Fields)
                {
                    if (fieldsByName.TryGetValue(field.Path, out Field inheritedField))
                    {
                        var newType = field.Type;
                        if (field.Type != inheritedField.Type && !IsAncestor(inheritedField.Type, field.Type))
                        {
                            var message = $"{cls.Iri} inherits property {field.Path} from {ancestor.Iri}, but tries to change its type from {inheritedField.Type} to {field.Type}";
                            throw new NotImplementedException(message);
                        }
                        Cardinality newCardinality;
                        try
                        {
                            newCardinality = field.Cardinality.Intersection(inheritedField.Cardinality);
                        }
                        catch (EmptyIntersectionException)
                        {
                            var message = $"{cls.Iri} inherits property {field.Path} from {ancestor.Iri}, but tries to change its cardinality from {inheritedField.Cardinality} to {field.Cardinality}";
                            throw new NotImplementedException(message);
                        }
                        fieldsByName[field.Path] = new Field(field.Path, field.Type, newCardinality);
                    }
                    else
                    {
                        fieldNamesOrder.Add(field.Path);
                        fieldsByName[field.Path] = field;
                    }
                }
            }
            return fieldNamesOrder.Select((iri) => fieldsByName[iri]).Where((field) => field.Cardinality != Cardinality.Zero);
        }

        private bool IsAncestor(Iri ancestor, Iri descendant)
        {
            return GetAncestors(ClassesByIri[descendant]).Contains(ClassesByIri[ancestor]);
        }

        private List<Class> GetAncestors(Class cls)
        {
            var ancestors = new List<Class>();
            while (true)
            {
                ancestors.Add(cls);
                if (cls.SubclassOf == null)
                    break;
                if (ClassesByIri.TryGetValue(cls.SubclassOf, out Class parent))
                    cls = parent;
                else
                    break;
            }
            return ancestors;
        }
    }
}