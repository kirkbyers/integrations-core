# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import pytest

from datadog_checks.ibm_db2 import IbmDb2Check

pytestmark = pytest.mark.integration


def test_check(aggregator, instance):
    check = IbmDb2Check('ibm_db2', {}, {})
    check.check(instance)

    aggregator.assert_all_metrics_covered()
