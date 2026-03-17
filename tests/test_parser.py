# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_liver_tox

from typing import Any

import pytest

from coreason_etl_liver_tox.parser import StructuralXMLTransmutationTask, XMLParsingError


def test_transmute_valid_xml() -> None:
    """AGENT INSTRUCTION: Ensure that a valid, nested XML payload is correctly parsed
    into dictionaries, enforcing force_list."""
    xml_payload = """
    <pmc-book-data>
        <BookDocument>
            <ArticleIdList>
                <ArticleId IdType="bookaccession">NBK547852</ArticleId>
            </ArticleIdList>
            <Book>
                <BookTitle>Acetaminophen</BookTitle>
            </Book>
            <Body>
                <sec>
                    <title>Hepatotoxicity</title>
                    <p>Acetaminophen is a well-known cause of liver injury.</p>
                    <p>Likelihood score: A</p>
                </sec>
                <sec>
                    <title>Mechanism of Injury</title>
                    <p>The mechanism involves NAPQI accumulation.</p>
                </sec>
            </Body>
        </BookDocument>
    </pmc-book-data>
    """
    documents = StructuralXMLTransmutationTask.transmute(xml_payload)

    assert isinstance(documents, list)
    assert len(documents) == 1

    doc = documents[0]
    # Check force_list coercion
    assert isinstance(doc["Body"]["sec"], list)
    assert len(doc["Body"]["sec"]) == 2
    assert isinstance(doc["Body"]["sec"][0]["p"], list)


def test_transmute_empty_xml() -> None:
    """AGENT INSTRUCTION: Ensure that an empty or malformed XML string raises an XMLParsingError."""
    with pytest.raises(XMLParsingError):
        StructuralXMLTransmutationTask.transmute("<malformed><unclosed>")


def test_transmute_empty_list_xml() -> None:
    """AGENT INSTRUCTION: Ensure that an empty pmc-book-data returns empty list."""
    xml_payload = """
    <pmc-book-data>
    </pmc-book-data>
    """
    documents = StructuralXMLTransmutationTask.transmute(xml_payload)
    assert documents == []


def test_transmute_pmc_list_empty_first_element() -> None:
    """AGENT INSTRUCTION: Ensure that an empty pmc-book-data list element is handled."""
    # Force pmc-book-data to be evaluated as empty inside the list logic
    xml_payload = """
    <root>
        <pmc-book-data></pmc-book-data>
        <pmc-book-data><BookDocument/></pmc-book-data>
    </root>
    """
    # This will trigger the `if not pmc_book_data: return []`
    documents = StructuralXMLTransmutationTask.transmute(xml_payload)
    assert documents == []


def test_transmute_pmc_list_explicit_none() -> None:
    """AGENT INSTRUCTION: Test case where pmc-book-data evaluates to None."""

    class MockXmltodict:
        @staticmethod
        def parse(*_args: Any, **_kwargs: Any) -> Any:
            return {"pmc-book-data": [None]}

    import xmltodict

    original_parse = xmltodict.parse
    xmltodict.parse = MockXmltodict.parse

    try:
        documents = StructuralXMLTransmutationTask.transmute("<dummy/>")
        assert documents == []
    finally:
        xmltodict.parse = original_parse


def test_transmute_pmc_list_explicit_none_no_list() -> None:
    """AGENT INSTRUCTION: Test case where pmc-book-data evaluates to None directly."""

    class MockXmltodict:
        @staticmethod
        def parse(*_args: Any, **_kwargs: Any) -> Any:
            return {"pmc-book-data": None}

    import xmltodict

    original_parse = xmltodict.parse
    xmltodict.parse = MockXmltodict.parse

    try:
        documents = StructuralXMLTransmutationTask.transmute("<dummy/>")
        assert documents == []
    finally:
        xmltodict.parse = original_parse


def test_transmute_pmc_list_with_empty_string() -> None:
    """AGENT INSTRUCTION: Ensure that an empty pmc-book-data as a list with an empty string is handled."""
    xml_payload = """
    <root>
        <pmc-book-data></pmc-book-data>
    </root>
    """
    documents = StructuralXMLTransmutationTask.transmute(xml_payload)
    assert len(documents) == 0


def test_transmute_pmc_list() -> None:
    """AGENT INSTRUCTION: Ensure that a pmc-book-data as a list is handled."""
    xml_payload = """
    <root>
        <pmc-book-data>
            <BookDocument>
            </BookDocument>
        </pmc-book-data>
        <pmc-book-data>
        </pmc-book-data>
    </root>
    """
    documents = StructuralXMLTransmutationTask.transmute(xml_payload)
    assert len(documents) > 0


def test_isolate_text_blocks() -> None:
    """AGENT INSTRUCTION: Test the extraction of specific structural text nodes
    (UID, Title, Hepatotoxicity, Mechanism)."""
    document = {
        "ArticleIdList": {"ArticleId": [{"@IdType": "bookaccession", "#text": "NBK547852"}]},
        "Book": {"BookTitle": ["Acetaminophen"]},
        "Body": {
            "sec": [
                {"title": ["Hepatotoxicity Summary"], "p": ["This drug causes DILI.", "Likelihood score: A[HD]"]},
                {
                    "title": ["Mechanism of Injury"],
                    "p": ["It involves metabolic activation."],
                    "sec": [{"title": ["Mechanism of Injury Detail"], "p": ["Nested Detail."]}],
                },
            ]
        },
    }

    result = StructuralXMLTransmutationTask.isolate_text_blocks(document)

    assert result["uid"] == "NBK547852"
    assert result["agent_name"] == "Acetaminophen"
    assert result["hepatotoxicity_summary"] == "This drug causes DILI. Likelihood score: A[HD]"
    assert result["likelihood_score_block"] == "This drug causes DILI. Likelihood score: A[HD]"
    assert result["mechanism_of_injury"] == "It involves metabolic activation."


def test_isolate_text_blocks_missing_fields() -> None:
    """AGENT INSTRUCTION: Test the extraction handles missing fields gracefully."""
    document = {"Book": {"BookTitle": ["Unknown Drug"]}}

    result = StructuralXMLTransmutationTask.isolate_text_blocks(document)

    assert result["uid"] is None
    assert result["agent_name"] == "Unknown Drug"
    assert result["hepatotoxicity_summary"] is None
    assert result["mechanism_of_injury"] is None


def test_isolate_text_blocks_exceptions(caplog: pytest.LogCaptureFixture) -> None:
    """AGENT INSTRUCTION: Test that parsing exceptions are caught."""
    # This will trigger an AttributeError on 'get' in extract UID
    # To hit the except block in extract UID we need `pmid_blocks` to be evaluated,
    # and then one of them to raise an error.

    # We pass a custom object that raises an exception when get is called
    class ExplodingDict(dict[str, Any]):
        def get(self, *_args: Any, **_kwargs: Any) -> Any:
            raise Exception("Boom!")

    document = {"ArticleIdList": {"ArticleId": [ExplodingDict()]}, "Book": ExplodingDict(), "Abstract": "not-a-dict"}

    document2 = {"ArticleIdList": {"ArticleId": ExplodingDict()}, "Book": ExplodingDict(), "Abstract": "not-a-dict"}

    with caplog.at_level("DEBUG"):
        result = StructuralXMLTransmutationTask.isolate_text_blocks(document)
        result2 = StructuralXMLTransmutationTask.isolate_text_blocks(document2)

    assert result["uid"] is None
    assert result["agent_name"] is None
    assert result2["uid"] is None
    assert result2["agent_name"] is None


def test_isolate_text_blocks_abstract_fallback() -> None:
    """AGENT INSTRUCTION: Test abstract fallback"""
    document = {
        "Abstract": {"AbstractText": [{"sec": [{"title": ["Hepatotoxicity Summary"], "p": ["From Abstract."]}]}]}
    }

    result = StructuralXMLTransmutationTask.isolate_text_blocks(document)
    assert result["hepatotoxicity_summary"] == "From Abstract."


def test_isolate_text_blocks_dict_traverse() -> None:
    """AGENT INSTRUCTION: Test dict traverse filtering"""
    document = {
        "Abstract": {
            "AbstractText": [
                "not-a-dict",
                {"sec": ["not-a-dict", {"title": ["Hepatotoxicity Summary"], "p": [123, "From Abstract."]}]},
            ]
        }
    }

    result = StructuralXMLTransmutationTask.isolate_text_blocks(document)
    assert result["hepatotoxicity_summary"] == "From Abstract."
