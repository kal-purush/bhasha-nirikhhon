import copy
import numpy as np
import pytest

from simuran.core.base_class import BaseSimuran
from simuran.core.base_container import GenericContainer


class FakeLoader(BaseSimuran):
    def load(self):
        self.data = self.attrs.get("fake", "default")

    def is_loaded(self):
        return self.data is not None


@pytest.mark.parametrize(
    "num_items, values",
    [
        (3, ("apple", "banana", "apple")),
        (5, (1, "orange", 30, (2, 3), "hi")),
    ],
)
class TestContainerSetup:
    @pytest.fixture
    def container(self, values):
        container_ = GenericContainer()
        for val in values:
            new = FakeLoader(dict(fake=val))
            container_.append(new)
        container_.load()
        return container_

    def test_container_create(self, num_items, values, container):
        assert len(container) == num_items

    def test_container_load(self, num_items, values, container):
        assert all(c.is_loaded() for c in container)

    def test_container_single_load(self, num_items, values, container):
        container.append(FakeLoader())
        item = container.load(num_items)
        assert item.is_loaded()

    def test_container_group(self, num_items, values, container):
        test_val = values[0]
        _, indices = container.group_by_property("data", test_val)
        group_split = container.split_into_groups("data")
        matches = [i for i in range(num_items) if values[i] == test_val]
        assert indices == matches
        assert group_split[test_val][1] == matches

    def test_container_values(self, num_items, values, container):
        data = container.get_property("data")
        assert data == list(values)

        data = container.get_possible_values("data")
        assert data == set(values)


if __name__ == "__main__":
    test_numpy_container()