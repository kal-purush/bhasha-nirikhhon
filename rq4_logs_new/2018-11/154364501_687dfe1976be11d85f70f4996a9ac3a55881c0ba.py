from New.DsnRoot import BlockRoot
from New.BlockValue import BlockValue
from New.RubriqueValue import RubriqueValue


class BlockInstance(BlockRoot):

    def __init__(self, block_value):
        self.block_value = block_value
        self.rubriques = [RubriqueValue(r) for r in self.block_value.block_conf.rubriques]
        self.sub_blocks = [BlockValue(b, self) for b in self.block_value.block_conf]

    def iterate_on_list(self):
        return self.sub_blocks

    def type_(self):
        return self.block_value.type_()

    def name(self):
        result = self.block_value.name()
        result += ('/' if result else '') + self.type_().name
        if len(self.block_value.instances) > 1:
            result += '_' + str(self.block_value.instances.index(self) + 1)
        return result