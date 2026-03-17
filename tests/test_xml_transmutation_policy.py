# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module tests the XML-to-Dictionary transmutation policy.
"""

import pytest

from coreason_etl_liver_tox.utils.xml_transmutation_policy import EpistemicXmlTransmutationPolicy


def test_transmute_xml_to_dict_forces_lists() -> None:
    xml_payload = """
    <root>
        <sec>
            <title>Single Title</title>
            <p>Single paragraph</p>
        </sec>
    </root>
    """
    result = EpistemicXmlTransmutationPolicy.transmute_xml_to_dict(xml_payload)

    # Asserting that FRD Rule 1 (force_list=('sec', 'p', 'title', 'table', 'list', 'item')) is respected.
    assert isinstance(result["root"]["sec"], list), "Expected 'sec' to be forced into a list"
    assert len(result["root"]["sec"]) == 1

    sec = result["root"]["sec"][0]
    assert isinstance(sec["title"], list), "Expected 'title' to be forced into a list"
    assert isinstance(sec["p"], list), "Expected 'p' to be forced into a list"
    assert sec["title"][0] == "Single Title"
    assert sec["p"][0] == "Single paragraph"


def test_transmute_xml_to_dict_handles_multiple_elements() -> None:
    xml_payload = """
    <root>
        <sec>
            <title>Section 1</title>
            <p>Paragraph 1</p>
            <p>Paragraph 2</p>
        </sec>
        <sec>
            <title>Section 2</title>
        </sec>
    </root>
    """
    result = EpistemicXmlTransmutationPolicy.transmute_xml_to_dict(xml_payload)

    secs = result["root"]["sec"]
    assert isinstance(secs, list)
    assert len(secs) == 2
    assert secs[0]["title"][0] == "Section 1"
    assert len(secs[0]["p"]) == 2
    assert secs[1]["title"][0] == "Section 2"


def test_transmute_xml_to_dict_invalid_xml() -> None:
    with pytest.raises(ValueError, match="Failed to transmute XML payload"):
        EpistemicXmlTransmutationPolicy.transmute_xml_to_dict("<root><unclosed>")


def test_isolate_clinical_text_blocks() -> None:
    parsed_dict = {
        "root": {
            "sec": [
                {"title": ["Introduction"], "p": ["This is the intro."]},
                {"title": ["Hepatotoxicity"], "p": ["This drug causes liver injury."]},
                {"title": ["Likelihood Score"], "p": ["Likelihood score: Category A[HD]"]},
                {"title": ["Mechanism of Injury"], "p": ["Mechanism of injury."]},
            ]
        }
    }

    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)

    # Assert that it correctly extracts text from the dictionary containing the relevant sections.
    assert "Hepatotoxicity This drug causes liver injury." in isolated_blocks["hepatotoxicity_summary"]
    assert "Likelihood Score Likelihood score: Category A[HD]" in isolated_blocks["likelihood_score"]
    assert "Mechanism of Injury Mechanism of injury." in isolated_blocks["mechanism_of_injury"]


def test_isolate_clinical_text_blocks_nested() -> None:
    parsed_dict = {
        "root": {
            "book": {
                "chapter": {
                    "sec": [
                        {"title": ["Hepatotoxicity"], "sec": [{"p": ["Nested hepatotoxicity info."]}]},
                        {"sec": [{"p": ["The likelihood score is Category B."]}]},
                    ]
                }
            }
        }
    }

    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)

    assert "Nested hepatotoxicity info." in isolated_blocks["hepatotoxicity_summary"]
    assert "The likelihood score is Category B." in isolated_blocks["likelihood_score"]


def test_isolate_clinical_text_blocks_not_found() -> None:
    parsed_dict = {"root": {"sec": [{"title": ["Introduction"], "p": ["This is the intro."]}]}}

    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)
    assert isolated_blocks["hepatotoxicity_summary"] is None
    assert isolated_blocks["likelihood_score"] is None
    assert isolated_blocks["mechanism_of_injury"] is None


def test_extract_text_ignores_attributes() -> None:
    parsed_dict = {
        "root": {"sec": [{"@id": "sec-1", "title": ["Hepatotoxicity"], "p": ["This drug causes liver injury."]}]}
    }
    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)
    assert "Hepatotoxicity This drug causes liver injury." in isolated_blocks["hepatotoxicity_summary"]
    assert "sec-1" not in isolated_blocks["hepatotoxicity_summary"]


def test_extract_text_empty_node() -> None:
    parsed_dict = {"root": {"sec": [{"title": ["Hepatotoxicity"], "p": []}]}}
    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)
    assert "Hepatotoxicity" in isolated_blocks["hepatotoxicity_summary"]


def test_find_all_sections_str() -> None:
    # To cover `elif isinstance(node, list)` with strings or dicts inside lists properly
    # and cover line 86, 102, 110
    parsed_dict = {"root": ["string in list", {"sec": {"title": ["Hepatotoxicity"], "p": ["Test."]}}]}
    isolated_blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(parsed_dict)
    assert "Hepatotoxicity Test." in isolated_blocks["hepatotoxicity_summary"]


def test_extract_text_none_or_int() -> None:
    # What if a node contains an int or None instead of str/dict/list?
    # Though xmltodict usually returns strings, we should handle unexpected types for coverage.
    assert EpistemicXmlTransmutationPolicy._extract_text(None) == ""
    assert EpistemicXmlTransmutationPolicy._extract_text(123) == ""
