# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""This module contains code to test SageMaker ``LineageQueryResult.visualize()``"""
from __future__ import absolute_import
import time
import os

import pytest

import sagemaker.lineage.query
from sagemaker.lineage.query import LineageQueryDirectionEnum
from tests.integ.sagemaker.lineage.helpers import name, LineageResourceHelper


def test_LineageResourceHelper(sagemaker_session):
    # check if LineageResourceHelper works properly
    lineage_resource_helper = LineageResourceHelper(sagemaker_session=sagemaker_session)
    try:
        art1 = lineage_resource_helper.create_artifact(artifact_name=name())
        art2 = lineage_resource_helper.create_artifact(artifact_name=name())
        lineage_resource_helper.create_association(source_arn=art1, dest_arn=art2)
    except Exception as e:
        print(e)
        assert False
    finally:
        lineage_resource_helper.clean_all()

# @pytest.mark.skip("query load test")
def test_large_query(sagemaker_session):
    lineage_resource_helper = LineageResourceHelper(sagemaker_session=sagemaker_session)
    large_data_root_arn = lineage_resource_helper.create_artifact(artifact_name=name())

    # create large lineage data
    # Artifact ----> Artifact
    #        \ \ \-> Artifact
    #         \ \--> Artifact
    #          \--->  ...
    try:
        for i in range(150):
            artifact_arn = lineage_resource_helper.create_artifact(artifact_name=name())
            lineage_resource_helper.create_association(
                source_arn=large_data_root_arn, dest_arn=artifact_arn
            )

        lq = sagemaker.lineage.query.LineageQuery(sagemaker_session)
        lq_result = lq.query(start_arns=[large_data_root_arn])
        assert len(lq_result.edges) == 150

    except Exception as e:
        print(e)
        assert False

    finally:
        lineage_resource_helper.clean_all()