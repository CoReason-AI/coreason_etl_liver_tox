# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

import pytest

from coreason_etl_liver_tox.parser import (
    isolate_hepatotoxicity_summary,
    isolate_likelihood_score,
    parse_livertox_xml,
)


def test_parse_livertox_xml_forces_lists() -> None:
    """Verify that parse_livertox_xml correctly applies force_list to configured tags."""
    xml_data = b"""
    <BookData>
        <Chapter>
            <sec>
                <title>Hepatotoxicity</title>
                <p>Paragraph 1</p>
            </sec>
        </Chapter>
    </BookData>
    """
    parsed = parse_livertox_xml(xml_data)

    # 'sec', 'title', and 'p' must be lists even though there is only one
    chapter = parsed["BookData"]["Chapter"]
    assert isinstance(chapter["sec"], list)
    assert isinstance(chapter["sec"][0]["title"], list)
    assert isinstance(chapter["sec"][0]["p"], list)


def test_parse_livertox_xml_invalid_bytes() -> None:
    """Verify that parsing invalid XML bytes raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid XML payload"):
        parse_livertox_xml(b"<UnclosedTag>")


def test_isolate_hepatotoxicity_summary() -> None:
    """Verify isolation of the Hepatotoxicity summary section."""
    record_dict = {
        "BookData": {
            "Chapter": {
                "sec": [
                    {"title": ["Background"], "p": ["Drug X is used for..."]},
                    {
                        "title": ["Hepatotoxicity"],
                        "p": ["Drug X can cause severe injury."],
                        "sec": [{"title": ["Case Report"], "p": ["A 50 year old..."]}],
                    },
                ]
            }
        }
    }

    summary = isolate_hepatotoxicity_summary(record_dict)
    assert summary is not None
    assert "Drug X can cause severe injury." in summary
    assert "Case Report A 50 year old" in summary
    assert "Hepatotoxicity" not in summary  # Title should be excluded


def test_isolate_hepatotoxicity_summary_not_found() -> None:
    """Verify behavior when Hepatotoxicity section is missing."""
    record_dict = {"BookData": {"Chapter": {"sec": [{"title": ["Background"], "p": ["Drug X is used for..."]}]}}}

    summary = isolate_hepatotoxicity_summary(record_dict)
    assert summary is None


def test_isolate_likelihood_score() -> None:
    """Verify isolation of the Likelihood score text block."""
    record_dict = {
        "BookData": {
            "Chapter": {
                "sec": [
                    {
                        "title": ["Hepatotoxicity"],
                        "p": ["Drug X can cause severe injury."],
                        "sec": [
                            {
                                "title": ["Product Info"],
                                "p": ["Likelihood score: Category A[HD] (well known cause).", "Other info."],
                            }
                        ],
                    }
                ]
            }
        }
    }

    score_text = isolate_likelihood_score(record_dict)
    assert score_text is not None
    assert "Category A[HD]" in score_text


def test_isolate_likelihood_score_not_found() -> None:
    """Verify behavior when Likelihood score is missing."""
    record_dict = {
        "BookData": {"Chapter": {"sec": [{"title": ["Hepatotoxicity"], "p": ["Drug X can cause severe injury."]}]}}
    }

    score_text = isolate_likelihood_score(record_dict)
    assert score_text is None
