import { FamilyNode, Gender, RawFamilyMember } from '@/features/family-tree/data/family';

class FamilyNodeFactory {
    public createNode(id: string, name: string, gender: Gender, options: Partial<FamilyNode> = {}): FamilyNode {
        return {
            id,
            name,
            gender,
            parentId: options.parentId,
            spouse: options.spouse,
            address: options.address,
            birthPlace: options.birthPlace,
            birthDate: options.birthDate,
            generation: options.generation ?? 1,
            label: options.label,
            children: options.children ?? [],
            notes: options.notes,
            familyId: options.familyId,
            status: options.status,
        };
    }

    public createFromRaw(raw: RawFamilyMember): FamilyNode {
        return this.createNode(raw.id.toString(), raw.name, raw.gender, {
            parentId: raw.parent_id?.toString(),
            spouse: raw.spouse || undefined,
            address: raw.address || undefined,
            birthPlace: raw.birth_place || undefined,
            birthDate: raw.birth_date || undefined,
            generation: raw.generation,
            label: raw.label || undefined,
            children: raw.children ? raw.children.map((child: RawFamilyMember) => this.createFromRaw(child)) : [],
        });
    }
}

export { FamilyNodeFactory };