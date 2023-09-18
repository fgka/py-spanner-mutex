# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Configurations."""
from typing import Any, List, Tuple

import attrs

from py_spanner_mutex.common import const, dto_defaults, preprocess


@attrs.define(**const.ATTRS_DEFAULTS)
class ZoneMap(dto_defaults.HasFromJsonString):
    """How are zones mapped from source to target."""

    source: str = attrs.field(validator=attrs.validators.instance_of(str))
    target: str = attrs.field(validator=attrs.validators.instance_of(str))

    @source.validator
    def _source_validator(self, attribute: attrs.Attribute, value: Any) -> None:
        self._zone_validator(attribute.name, value)

    @target.validator
    def _target_validator(self, attribute: attrs.Attribute, value: Any) -> None:
        self._zone_validator(attribute.name, value)

    @staticmethod
    def _zone_validator(name: str, value: str) -> None:
        preprocess.string(value, name, regex=const.FQN_ZONE_REGEX)


@attrs.define(**const.ATTRS_DEFAULTS)
class SubnetMap(dto_defaults.HasFromJsonString):
    """How are subnets mapped from source to target."""

    source: str = attrs.field(validator=attrs.validators.instance_of(str))
    target: str = attrs.field(validator=attrs.validators.instance_of(str))
    zone_mapping: List[ZoneMap] = attrs.field(
        validator=attrs.validators.and_(
            attrs.validators.deep_iterable(member_validator=attrs.validators.instance_of(ZoneMap)),
            attrs.validators.min_len(1),
        ),
    )

    @source.validator
    def _source_validator(self, attribute: attrs.Attribute, value: Any) -> None:
        self._subnet_validator(attribute.name, value)

    @target.validator
    def _target_validator(self, attribute: attrs.Attribute, value: Any) -> None:
        self._subnet_validator(attribute.name, value)

    @staticmethod
    def _subnet_validator(name: str, value: str) -> None:
        preprocess.string(value, name, regex=const.FQN_SUBNET_REGEX)

    def source_components(self) -> Tuple[str, str, str]:
        """
        Returns the components of the source subnet::
            src_project, src_region, src_name = subnet_map.source_components()
        Returns:
            A py:cls:`tuple` in the format ``(<project>, <region>, <name>)``.
        """
        return self._subnet_components(self.source)

    @staticmethod
    def _subnet_components(value: str) -> Tuple[str, str, str]:
        return const.FQN_SUBNET_REGEX.match(value).groups()

    def target_components(self) -> Tuple[str, str, str]:
        """
        Returns the components of the target subnet::
            tgt_project, tgt_region, tgt_name = subnet_map.target_components()
        Returns:
            A py:cls:`tuple` in the format ``(<project>, <region>, <name>)``.
        """
        return self._subnet_components(self.target)


@attrs.define(**const.ATTRS_DEFAULTS)
class MigrationTarget(dto_defaults.HasFromJsonString):
    """
    Defines a migration target.
    """

    migration: List[SubnetMap] = attrs.field(
        validator=attrs.validators.and_(
            attrs.validators.deep_iterable(member_validator=attrs.validators.instance_of(SubnetMap)),
            attrs.validators.min_len(1),
        )
    )
    state_file: str = attrs.field(validator=attrs.validators.instance_of(str))

    @state_file.validator
    def _state_file_validator(self, attribute: attrs.Attribute, value: Any) -> None:
        preprocess.path(value, attribute.name, exists=False, is_file=True, can_write=True)
