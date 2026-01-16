from ftw.builder import builder_registry
from ftw.builder.archetypes import ArchetypesBuilder


class TempFolderBuilder(ArchetypesBuilder):

    portal_type = 'TempFolder'


builder_registry.register('temp folder', TempFolderBuilder)