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

import xmltodict

from coreason_etl_liver_tox.utils.logger import logger


class XMLParsingError(Exception):
    """AGENT INSTRUCTION: Exception raised for structural parsing failures during Transmutation."""


class StructuralXMLTransmutationTask:
    """
    AGENT INSTRUCTION: This class manages the transmutation of raw NCBI XML into safe,
    structurally stable Python dictionaries. It strictly enforces the `force_list` rule
    to prevent scalar vs. array schema mutations in downstream consumers.
    """

    FORCE_LIST_TUPLE = ("sec", "p", "title", "table", "list", "item", "BookDocument")

    @classmethod
    def transmute(cls, xml_payload: str) -> list[dict[str, Any]]:
        """
        AGENT INSTRUCTION: Convert an XML string payload into a list of parsed document dictionaries.
        Extracts individual 'BookDocument' entities from an 'efetch' response wrapper.
        """
        try:
            parsed = xmltodict.parse(xml_payload, force_list=cls.FORCE_LIST_TUPLE, dict_constructor=dict)

            # If wrapped in a generic root node, extract it
            if "pmc-book-data" not in parsed:
                for key in parsed:
                    if isinstance(parsed[key], dict) and "pmc-book-data" in parsed[key]:
                        parsed = parsed[key]
                        break

            # Navigate to the BookDocument array
            pmc_book_data = parsed.get("pmc-book-data", {})
            if not pmc_book_data:
                return []

            if isinstance(pmc_book_data, list):
                pmc_book_data = pmc_book_data[0]  # Root is usually singular but force_list might affect it

            if not pmc_book_data:
                return []

            return list(pmc_book_data.get("BookDocument", []))
        except Exception as e:
            logger.error(f"XML Parsing failed: {e}")
            raise XMLParsingError(f"Failed to parse XML payload: {e}") from e

    @classmethod
    def isolate_text_blocks(cls, document: dict[str, Any]) -> dict[str, str | None]:
        """
        AGENT INSTRUCTION: Recursively searches the parsed document for specific 'sec' (section)
        elements based on their 'title'. Extracts the raw text for downstream SQL parsing.
        """
        result: dict[str, str | None] = {
            "uid": None,
            "agent_name": None,
            "hepatotoxicity_summary": None,
            "likelihood_score_block": None,
            "mechanism_of_injury": None,
        }

        # Extract UID safely
        try:
            pmid_blocks = document.get("ArticleIdList", {}).get("ArticleId", [])
            if not isinstance(pmid_blocks, list):
                pmid_blocks = [pmid_blocks]

            for pmid in pmid_blocks:
                if isinstance(pmid, dict) and pmid.get("@IdType") == "bookaccession":
                    result["uid"] = pmid.get("#text")
                    break
        except Exception as e:
            logger.debug(f"Error extracting UID: {e}")

        # Extract Title (Agent Name)
        try:
            title_blocks = document.get("Book", {}).get("BookTitle", [])
            if title_blocks and len(title_blocks) > 0:
                result["agent_name"] = title_blocks[0]
        except Exception as e:
            logger.debug(f"Error extracting Agent Name: {e}")

        # Isolate text blocks recursively
        def traverse_sections(sections: list[Any]) -> None:
            for sec in sections:
                if not isinstance(sec, dict):
                    continue

                title_list = sec.get("title", [])
                if title_list and len(title_list) > 0:
                    title_text = title_list[0].lower().strip() if isinstance(title_list[0], str) else ""

                    # Accumulate all paragraph text in this section
                    paragraphs = sec.get("p", [])
                    text_content = " ".join([p for p in paragraphs if isinstance(p, str)])

                    if "hepatotoxicity" in title_text and result["hepatotoxicity_summary"] is None:
                        result["hepatotoxicity_summary"] = text_content
                        # Often the likelihood score is at the end of this summary
                        result["likelihood_score_block"] = text_content
                    elif "mechanism of injury" in title_text and result["mechanism_of_injury"] is None:
                        result["mechanism_of_injury"] = text_content

                # Recurse into nested sections
                nested_sec = sec.get("sec", [])
                if nested_sec:
                    traverse_sections(nested_sec)

        try:
            body = document.get("Abstract", {}).get("AbstractText", [])
            # Body isn't always the root. Sections can be nested here
            sections = []

            for abs_text in body:
                if isinstance(abs_text, dict):
                    sections.extend(abs_text.get("sec", []))

            if not sections:
                # Try looking at the Book body if abstract isn't used
                book_body = document.get("Body", {})
                if isinstance(book_body, dict):
                    sections = book_body.get("sec", [])

            traverse_sections(sections)

        except Exception as e:
            logger.debug(f"Error while traversing sections: {e}")

        return result
