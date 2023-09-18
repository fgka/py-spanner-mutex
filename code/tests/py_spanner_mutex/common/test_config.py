# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,attribute-defined-outside-init
# type: ignore
import os
import tempfile
from typing import Any, List

import pytest

from py_spanner_mutex.common import config

_TEST_ZONE_SRC: str = "src-region1-a"
_TEST_ZONE_TGT: str = "tgt-region2-b"


class TestZoneMap:
    def test_ctor_ok(self):
        # Given
        source = _TEST_ZONE_SRC
        target = _TEST_ZONE_TGT
        assert source != target
        # When
        result = config.ZoneMap(source=source, target=target)
        # Then
        assert isinstance(result, config.ZoneMap)
        assert result.source == source
        assert result.target == target

    @pytest.mark.parametrize(
        "source,target",
        [
            ("ab-region1-a", None),
            (None, "cd-region2-b"),
            (" ab-region1-a", "cd-region2-b"),  # space starting source
            ("ab-region1-a", "cd-region2-b "),  # space starting target
            ("ab-region1-a ", "cd-region2-b"),  # space trailing source
            ("ab-region1-a", "cd-region2-b "),  # space trailing target
            ("region1-a", "cd-region2-b"),  # missing country source
            ("ab-region1-a", "cd-b"),  # missing region target
            ("ab-region1-", "cd-region2-b"),  # missing zone source
            ("ab-region1-a", "--"),  # only separators target
        ],
    )
    def test_ctor_nok(self, source: Any, target: Any):
        # Given/When
        with pytest.raises(Exception):
            config.ZoneMap(source=source, target=target)


_TEST_SRC_PRJ: str = "my-project-src"
_TEST_TGT_PRJ: str = "my-project-tgt"
_TEST_SRC_REGION: str = "my-region-src"
_TEST_TGT_REGION: str = "my-region-tgt"
_TEST_SRC_SUBNET_NAME: str = "my-subnet-src"
_TEST_TGT_SUBNET_NAME: str = "my-subnet-tgt"

_TEST_SUBNET_TMPL: str = "projects/{}/regions/{}/subnetworks/{}"
_TEST_SUBNET_SRC: str = _TEST_SUBNET_TMPL.format(_TEST_SRC_PRJ, _TEST_SRC_REGION, _TEST_SRC_SUBNET_NAME)
_TEST_SUBNET_TGT: str = _TEST_SUBNET_TMPL.format(_TEST_TGT_PRJ, _TEST_TGT_REGION, _TEST_TGT_SUBNET_NAME)

_TEST_ZONE_MAPPING: List[config.ZoneMap] = [config.ZoneMap(source=_TEST_ZONE_SRC, target=_TEST_ZONE_TGT)]


class TestSubnetMap:
    def setup_method(self):
        self.instance = config.SubnetMap(
            source=_TEST_SUBNET_SRC, target=_TEST_SUBNET_TGT, zone_mapping=_TEST_ZONE_MAPPING
        )

    def test_ctor_ok(self):
        # Given
        source = _TEST_SUBNET_SRC
        target = _TEST_SUBNET_TGT
        assert _TEST_SUBNET_SRC != _TEST_SUBNET_TGT
        # When
        result = config.SubnetMap(source=source, target=target, zone_mapping=_TEST_ZONE_MAPPING)
        # Then
        assert isinstance(result, config.SubnetMap)
        assert result.source == source
        assert result.target == target
        assert result.zone_mapping == _TEST_ZONE_MAPPING

    def test_source_components_ok(self):
        # Given/When
        prj, region, name = self.instance.source_components()
        # Then
        assert prj == _TEST_SRC_PRJ
        assert region == _TEST_SRC_REGION
        assert name == _TEST_SRC_SUBNET_NAME

    def test_target_components_ok(self):
        # Given/When
        prj, region, name = self.instance.target_components()
        # Then
        assert prj == _TEST_TGT_PRJ
        assert region == _TEST_TGT_REGION
        assert name == _TEST_TGT_SUBNET_NAME

    @pytest.mark.parametrize(
        "source,target,zone_mapping",
        [
            (_TEST_SUBNET_SRC, _TEST_SUBNET_TGT, None),  # invalid zone-mapping
            (_TEST_SUBNET_SRC, None, _TEST_ZONE_MAPPING),  # invalid target
            (None, _TEST_SUBNET_TGT, _TEST_ZONE_MAPPING),  # invalid source
            (_TEST_SUBNET_SRC, _TEST_SUBNET_TGT, []),  # empty zone-mapping
            (_TEST_SUBNET_SRC, "", _TEST_ZONE_MAPPING),  # empty target
            ("", _TEST_SUBNET_TGT, _TEST_ZONE_MAPPING),  # empty source
            (_TEST_SUBNET_SRC, _TEST_SUBNET_TGT, ["abc"]),  # invalid zone-mapping
            (" " + _TEST_SUBNET_SRC, _TEST_SUBNET_TGT, _TEST_ZONE_MAPPING),  # invalid source
            (_TEST_SUBNET_SRC, _TEST_SUBNET_TGT + " ", _TEST_ZONE_MAPPING),  # invalid target
        ],
    )
    def test_ctor_nok(self, source: Any, target: Any, zone_mapping: Any):
        # Given/When
        with pytest.raises(Exception):
            config.SubnetMap(source=source, target=target, zone_mapping=zone_mapping)


_TEST_MIGRATION_LST: List[config.SubnetMap] = [
    config.SubnetMap(source=_TEST_SUBNET_SRC, target=_TEST_SUBNET_TGT, zone_mapping=_TEST_ZONE_MAPPING)
]
_TEST_STATE_FILE: str = tempfile.NamedTemporaryFile(delete=True).name  # pylint: disable=consider-using-with


class TestMigrationTarget:
    def test_ctor_ok(self):
        # Given/When
        result = config.MigrationTarget(migration=_TEST_MIGRATION_LST, state_file=_TEST_STATE_FILE)
        # Then
        assert isinstance(result, config.MigrationTarget)
        assert result.migration == _TEST_MIGRATION_LST
        assert result.state_file == _TEST_STATE_FILE

    @pytest.mark.parametrize(
        "migration,state_file",
        [
            (None, _TEST_STATE_FILE),  # missing migration
            ([], _TEST_STATE_FILE),  # empty migration
            ([123], _TEST_STATE_FILE),  # wrong type migration
            (_TEST_MIGRATION_LST, None),  # wrong state file
            (_TEST_MIGRATION_LST, ""),  # wrong state file
            (_TEST_MIGRATION_LST, tempfile.mkdtemp()),  # dir as state file
        ],
    )
    def test_ctor_nok(self, migration: Any, state_file: Any):
        with pytest.raises(Exception):
            config.MigrationTarget(migration=migration, state_file=state_file)

    def test_ctor_nok_ro_state_file(self):
        # Given
        migration = _TEST_MIGRATION_LST
        state_file = tempfile.NamedTemporaryFile(delete=False).name  # pylint: disable=consider-using-with
        os.chmod(state_file, 0o400)
        # When/Then
        with pytest.raises(Exception):
            config.MigrationTarget(migration=migration, state_file=state_file)
