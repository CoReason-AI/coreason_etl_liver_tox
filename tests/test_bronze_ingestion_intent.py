# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

import pytest
from pytest_mock import MockerFixture

from coreason_etl_liver_tox.bronze_ingestion_intent import _extract_book_documents, _extract_uid, livertox_resource


@pytest.fixture
def mock_esearch_response() -> dict[str, dict[str, str]]:
    return {
        "esearchresult": {
            "count": "2",
            "querykey": "1",
            "webenv": "test_webenv",
        }
    }


@pytest.fixture
def mock_efetch_xml_batch() -> str:
    return """<?xml version="1.0"?>
    <BookDocumentSet>
        <BookDocument>
            <article-id pub-id-type="pmid">123</article-id>
            <title>Test Book 1</title>
            <sec>
                <title>Hepatotoxicity</title>
                <p>Liver injury is severe.</p>
            </sec>
            <sec>
                <title>Likelihood score</title>
                <p>Category A</p>
            </sec>
        </BookDocument>
        <BookDocument>
            <article-id pub-id-type="pmid">456</article-id>
            <title>Test Book 2</title>
            <sec>
                <title>Hepatotoxicity</title>
                <p>Liver injury is mild.</p>
            </sec>
            <sec>
                <title>Likelihood score</title>
                <p>Category B</p>
            </sec>
        </BookDocument>
    </BookDocumentSet>
    """


def test_livertox_resource_success(
    mocker: MockerFixture, mock_esearch_response: dict[str, dict[str, str]], mock_efetch_xml_batch: str
) -> None:
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value=mock_esearch_response,
    )
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.efetch_xml_transmutation",
        return_value=mock_efetch_xml_batch,
    )
    mocker.patch("coreason_etl_liver_tox.bronze_ingestion_intent.settings.ncbi_retmax", 2)

    # Evaluate the pipeline locally using dlt test helpers if needed,
    # or just list the resource. DLT flattens lists if yielded from an item!
    # A single batch of 2 elements will be yielded by generator and unpacked by DLT.
    records = list(livertox_resource())

    assert len(records) == 2

    # Check first record
    # Due to recursive text extraction with titles, likelihood score might have "Likelihood score Category A"
    assert records[0]["uid"] == "123"
    assert "coreason_id" in records[0]
    assert "Category A" in records[0]["extracted_blocks"]["likelihood_score"]
    assert "Liver injury is severe" in records[0]["extracted_blocks"]["hepatotoxicity_summary"]

    # Check second record
    assert records[1]["uid"] == "456"
    assert "coreason_id" in records[1]
    assert "Category B" in records[1]["extracted_blocks"]["likelihood_score"]


def test_livertox_resource_empty_documents_in_batch(mocker: MockerFixture) -> None:
    # Coverage for empty documents in batch returning break
    mock_esearch_response = {
        "esearchresult": {
            "count": "2",
            "querykey": "1",
            "webenv": "test_webenv",
        }
    }
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value=mock_esearch_response,
    )
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.efetch_xml_transmutation",
        return_value="<empty></empty>",
    )
    mocker.patch("coreason_etl_liver_tox.bronze_ingestion_intent.settings.ncbi_retmax", 2)

    records = list(livertox_resource())
    assert len(records) == 0


def test_livertox_source() -> None:
    from coreason_etl_liver_tox.bronze_ingestion_intent import livertox_source

    # To cover the source
    src = livertox_source()
    assert src is not None


def test_livertox_resource_no_records(mocker: MockerFixture) -> None:
    empty_esearch = {
        "esearchresult": {
            "count": "0",
            "querykey": "1",
            "webenv": "test_webenv",
        }
    }
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value=empty_esearch,
    )

    records = list(livertox_resource())
    assert len(records) == 0

    # To properly evaluate coverage under dlt and its generator structure,
    # we can explicitly invoke the __wrapped__ generator, bypassing dlt.
    import coreason_etl_liver_tox.bronze_ingestion_intent as intent

    # Run the raw generator
    try:
        if hasattr(intent.livertox_resource, "__wrapped__"):
            gen = intent.livertox_resource.__wrapped__()
            next(gen)
    except StopIteration:
        pass


def test_livertox_resource_invalid_esearch(mocker: MockerFixture) -> None:
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value={"invalid": "response"},
    )

    with pytest.raises(Exception, match="Invalid esearch response format"):
        list(livertox_resource())


def test_livertox_resource_invalid_esearch_missing_key(mocker: MockerFixture) -> None:
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value={"esearchresult": {"count": "1"}},  # missing querykey and webenv
    )

    import inspect

    from coreason_etl_liver_tox import bronze_ingestion_intent

    original_func = inspect.unwrap(bronze_ingestion_intent.livertox_resource)
    gen = original_func()

    with pytest.raises(ValueError, match="Invalid esearch response format"):
        next(gen)


def test_extract_uid_variations() -> None:
    # Test article-id list
    doc_article_id_list = {"article-id": [{"#text": "111"}, {"#text": "222"}]}
    assert _extract_uid(doc_article_id_list) == "111"

    # Test article-id list with string
    doc_article_id_list_str = {"article-id": ["111", "222"]}
    assert _extract_uid(doc_article_id_list_str) == "111"

    # Test book-id list with string
    doc_book_id_list_str = {"book-id": ["666", "777"]}
    assert _extract_uid(doc_book_id_list_str) == "666"

    # Test article-id dict
    doc_article_id_dict = {"article-id": {"#text": "333"}}
    assert _extract_uid(doc_article_id_dict) == "333"

    # Test article-id str
    doc_article_id_str = {"article-id": "444"}
    assert _extract_uid(doc_article_id_str) == "444"

    # Test book-id dict
    doc_book_id_dict = {"book-id": {"#text": "555"}}
    assert _extract_uid(doc_book_id_dict) == "555"

    # Test book-id list
    doc_book_id_list = {"book-id": [{"#text": "666"}]}
    assert _extract_uid(doc_book_id_list) == "666"

    # Test book-id str
    doc_book_id_str = {"book-id": "777"}
    assert _extract_uid(doc_book_id_str) == "777"

    # Test simple id
    doc_id = {"id": "888"}
    assert _extract_uid(doc_id) == "888"

    # Test fallback hash
    doc_empty = {"some_other_key": "val"}
    uid = _extract_uid(doc_empty)
    assert len(uid) == 64  # sha256 hex length


def test_extract_book_documents_variations() -> None:
    # Test BookDocumentSet with list
    parsed1 = {"BookDocumentSet": {"BookDocument": [{"id": "1"}, {"id": "2"}]}}
    assert len(_extract_book_documents(parsed1)) == 2

    # Test BookDocumentSet with single dict
    parsed2 = {"BookDocumentSet": {"BookDocument": {"id": "1"}}}
    assert len(_extract_book_documents(parsed2)) == 1

    # Test pmc-articleset with list
    parsed3 = {"pmc-articleset": {"article": [{"id": "1"}, {"id": "2"}]}}
    assert len(_extract_book_documents(parsed3)) == 2

    # Test pmc-articleset with single dict
    parsed4 = {"pmc-articleset": {"article": {"id": "1"}}}
    assert len(_extract_book_documents(parsed4)) == 1

    # Test generic fallback with list
    parsed5 = {"unknown-root": [{"id": "1"}, {"id": "2"}]}
    assert len(_extract_book_documents(parsed5)) == 2

    # Test generic fallback with dict
    parsed6 = {"unknown-root": {"id": "1"}}
    assert len(_extract_book_documents(parsed6)) == 1


def test_livertox_resource_processing_error_continue(
    mocker: MockerFixture, mock_esearch_response: dict[str, dict[str, str]]
) -> None:
    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.esearch_history_manifold",
        return_value=mock_esearch_response,
    )

    # Provide one bad doc that will raise an error during uid extraction, and one good doc
    bad_xml = """<?xml version="1.0"?>
    <BookDocumentSet>
        <BookDocument>
            <bad-format>true</bad-format>
        </BookDocument>
        <BookDocument>
            <article-id pub-id-type="pmid">456</article-id>
        </BookDocument>
    </BookDocumentSet>
    """

    mocker.patch(
        "coreason_etl_liver_tox.utils.epistemic_eutilities_client_policy.EpistemicEUtilitiesClientPolicy.efetch_xml_transmutation",
        return_value=bad_xml,
    )

    # Make _extract_uid raise an exception for the first one to test the try/except continue
    orig_extract = _extract_uid

    def mock_extract(doc: dict[str, str]) -> str:
        if "bad-format" in doc:
            raise Exception("Bad doc")
        return orig_extract(doc)

    mocker.patch("coreason_etl_liver_tox.bronze_ingestion_intent._extract_uid", side_effect=mock_extract)

    records = list(livertox_resource())

    assert len(records) == 1
    assert records[0]["uid"] == "456"
