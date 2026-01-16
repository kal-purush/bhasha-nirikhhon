import {DynamicMetadataLoader} from '@anglr/dynamic';
import {RelationsEditorMetadataDescriptor} from '@anglr/dynamic/relations-editor';

/**
 * Drop empty relations metadata loader
 */
export const DropEmptyRelationsMetadataLoader: DynamicMetadataLoader<RelationsEditorMetadataDescriptor> = async () => new (await import('./metadata/dropEmpty.relationsMetadata')).DropEmptyRelationsEditorMetadata();