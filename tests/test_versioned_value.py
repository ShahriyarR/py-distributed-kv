import pytest

from pydistributedkv.domain.models import VersionedValue


class TestVersionedValue:
    def test_init(self):
        """Test VersionedValue initialization"""
        vv = VersionedValue(current_version=1, value="test")

        assert vv.current_version == 1
        assert vv.value == "test"
        assert vv.history is None

    def test_get_value_latest(self):
        """Test getting the latest value"""
        vv = VersionedValue(current_version=3, value="version3", history={1: "version1", 2: "version2"})

        # When version is None, it should return the latest value
        assert vv.get_value() == "version3"

    def test_get_value_specific_version(self):
        """Test getting a specific version"""
        vv = VersionedValue(current_version=3, value="version3", history={1: "version1", 2: "version2"})

        assert vv.get_value(version=2) == "version2"
        assert vv.get_value(version=1) == "version1"
        assert vv.get_value(version=3) == "version3"

    def test_get_value_nonexistent_version(self):
        """Test getting a version that doesn't exist"""
        vv = VersionedValue(current_version=3, value="version3", history={1: "version1", 2: "version2"})

        assert vv.get_value(version=4) is None
        assert vv.get_value(version=0) is None

    def test_update_next_version(self):
        """Test updating to the next consecutive version"""
        vv = VersionedValue(current_version=1, value="version1")

        vv.update("version2", 2)

        assert vv.current_version == 2
        assert vv.value == "version2"
        assert vv.history == {1: "version1"}

    def test_update_nonconsecutive_version(self):
        """Test updating with a gap in version numbers"""
        vv = VersionedValue(current_version=1, value="version1")

        vv.update("version3", 3)

        assert vv.current_version == 3
        assert vv.value == "version3"
        assert vv.history == {1: "version1"}

    def test_update_older_version(self):
        """Test that updating with an older version is ignored"""
        vv = VersionedValue(current_version=3, value="version3", history={1: "version1", 2: "version2"})

        vv.update("old_value", 2)

        # The update should be ignored
        assert vv.current_version == 3
        assert vv.value == "version3"
        assert 2 in vv.history
        assert vv.history[2] == "version2"

    def test_update_same_version(self):
        """Test that updating with the same version is ignored"""
        vv = VersionedValue(current_version=3, value="version3", history={1: "version1", 2: "version2"})

        vv.update("new_version3", 3)

        # The update should be ignored
        assert vv.current_version == 3
        assert vv.value == "version3"

    def test_multiple_updates(self):
        """Test multiple sequential updates"""
        vv = VersionedValue(current_version=1, value="version1")

        vv.update("version2", 2)
        vv.update("version3", 3)
        vv.update("version4", 4)

        assert vv.current_version == 4
        assert vv.value == "version4"
        assert vv.history == {1: "version1", 2: "version2", 3: "version3"}

        # Check that we can retrieve all versions
        assert vv.get_value(1) == "version1"
        assert vv.get_value(2) == "version2"
        assert vv.get_value(3) == "version3"
        assert vv.get_value(4) == "version4"
        assert vv.get_value() == "version4"
