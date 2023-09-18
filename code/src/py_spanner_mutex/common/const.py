# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Default values for creating an attributes class. To be used as::

import attrs

@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class MyAttrs: pass
"""
import re
from typing import Dict

############
#  Common  #
############

ENCODING_UTF8: str = "UTF-8"


###########
#  ATTRS  #
###########

ATTRS_DEFAULTS: Dict[str, bool] = dict(
    kw_only=True,
    str=True,
    repr=True,
    eq=True,
    hash=True,
    frozen=True,
    slots=True,
)

#####################
#  Name Validation  #
#####################

FQN_SUBNET_REGEX: re.Pattern = re.compile(
    pattern=r"^projects/([^/\s]+)/regions/([^/\s]+)/subnetworks/([^/\s]+)$",
    flags=re.IGNORECASE,
)
"""
Input example::
    projects/my-project/regions/my-region/subnetworks/my-subnet
Groups on matching are::
    project, region, name = FQN_SUBNET_REGEX.match(line).groups()
    # "my-project", "my-region", "my-subnet"
"""

FQN_ZONE_REGEX: re.Pattern = re.compile(
    pattern=r"^([^/\s]+)-([^/\s]+)-([^/\s]+)$",
    flags=re.IGNORECASE,
)
"""
Input example::
    us-central1-a
Groups on matching are::
    country, region, name = FQN_ZONE_REGEX.match(line).groups()
    # "us", "central1", "a"
"""

#####################
#  Migration Steps  #
#####################

STATE_START: str = "START"
STATE_CREATE_INVENTORY: str = "CREATE_INVENTORY"
STATE_DONE: str = "DONE"
STATE_FAILED: str = "FAILED"
STATE_ROLLBACK: str = "ROLLBACK"
