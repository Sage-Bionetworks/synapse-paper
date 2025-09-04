"""
Description: a script to extract all access approvals for access requirements

This script can only be executed by someone with ACT team privileges
"""

import json
import logging
from typing import Iterable

import synapseclient
from synapseclient import Synapse
import pandas as pd

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def _POST_paginated(syn, uri: str, body: dict, **kwargs) -> Iterable[dict]:
    """
    Get paginated results

    Arguments:
        uri:     A URI that returns paginated results
        body:    POST request payload

    Returns:
        A generator over some paginated results
    """

    next_page_token = None
    while True:
        body["nextPageToken"] = next_page_token
        response = syn.restPOST(uri, body=json.dumps(body), **kwargs)
        next_page_token = response.get("nextPageToken")
        for item in response["results"]:
            yield item
        if next_page_token is None:
            break


def get_submissions(syn: Synapse, access_requirement_id: str) -> list:
    """
    Function to retrieve a list of submissions to a given controlled access requirement id

    Arguments:
        access_requirement_id (str): a given controlled access requirement id

    Returns:
        a list of submissions for a controlled access requirement id
    """
    # get the access requirement lists for the access requirement id
    search_request = {"accessRequirementId": str(access_requirement_id)}
    submissions = _POST_paginated(
        syn=syn,
        uri=f"/accessRequirement/{str(access_requirement_id)}/submissions",
        body=search_request,
    )
    all_subs = [sub for sub in submissions]
    return all_subs


def main():
    # Set up a "readonly" synapse config profile
    syn = synapseclient.login(profile="readonly")
    all_submissions = []
    access_requirements = [9603055, 9606628]
    for access_requirement in access_requirements:
        submission = get_submissions(syn=syn, access_requirement_id=access_requirement)
        all_submissions.extend(submission)
    submission_df = pd.DataFrame(all_submissions)
    submission_df.to_csv("access_approvals.csv", index=False)


if __name__ == "__main__":
    main()
