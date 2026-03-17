# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.

"""
AGENT INSTRUCTION: This module provides the XML-to-Dictionary transmutation policy
and text block isolation rules to handle NCBI E-utilities XML payloads reliably.
"""

from typing import Any

import xmltodict

from coreason_etl_liver_tox.utils.logger import logger


class EpistemicXmlTransmutationPolicy:
    """
    AGENT INSTRUCTION: Encapsulates the configuration and execution policies for converting
    NCBI XML payloads into Python dictionaries, strictly enforcing list structures for
    variable recurring tags to prevent downstream scalar/array mutation crashes.
    """

    # MANDATORY force_list tags as per FRD Rule 1
    # This guarantees that downstream parsing tools do not crash due to scalar vs. array mutations.
    _FORCE_LIST_TAGS = ("sec", "p", "title", "table", "list", "item")

    @classmethod
    def transmute_xml_to_dict(cls, xml_payload: str) -> dict[str, Any]:
        """
        Transmutes raw XML into a Python dictionary, enforcing stable schema types for NCBI structures.
        """
        logger.debug("Transmuting XML payload to dictionary with forced list structures.")
        try:
            parsed_dict: dict[str, Any] = xmltodict.parse(
                xml_payload,
                force_list=cls._FORCE_LIST_TAGS,
            )
            return parsed_dict
        except Exception as e:
            logger.exception("Failed to transmute XML payload")
            raise ValueError(f"Failed to transmute XML payload: {e}") from e

    @classmethod
    def isolate_clinical_text_blocks(cls, parsed_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Traverses the parsed dictionary to extract and isolate the core clinical text blocks
        (e.g., Hepatotoxicity Summary, Likelihood Score) from deeply nested HTML-like NCBI tags.
        """
        # We need to traverse the dictionary and recursively search for specific texts.
        # This implementation traverses looking for 'sec' titles containing keywords,
        # or recursively extracts all text content under a section.

        isolated_blocks: dict[str, str | None] = {
            "hepatotoxicity_summary": None,
            "likelihood_score": None,
            "mechanism_of_injury": None,
        }

        # LiverTox XML structure typically has a root element, then a book, then sections.
        # This recursive generator yields all matching titles and their parent sections.
        for sec in cls._find_all_sections(parsed_dict):
            title = cls._extract_text(sec.get("title", ""))

            if "hepatotoxicity" in title.lower():
                isolated_blocks["hepatotoxicity_summary"] = cls._extract_text(sec)
            elif "likelihood score" in title.lower() or "likelihood score" in cls._extract_text(sec).lower():
                # Sometimes the score is in its own section, sometimes embedded.
                isolated_blocks["likelihood_score"] = cls._extract_text(sec)
            elif "mechanism of injury" in title.lower():
                isolated_blocks["mechanism_of_injury"] = cls._extract_text(sec)

        return isolated_blocks

    @classmethod
    def _find_all_sections(cls, node: Any) -> list[dict[str, Any]]:
        """Recursively finds all 'sec' nodes in the dictionary."""
        sections = []
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "sec":
                    if isinstance(value, list):
                        sections.extend(value)
                    else:
                        sections.append(value)
                sections.extend(cls._find_all_sections(value))
        elif isinstance(node, list):
            for item in node:
                sections.extend(cls._find_all_sections(item))
        return sections

    @classmethod
    def _extract_text(cls, node: Any) -> str:
        """Recursively extracts all text values from a node, ignoring tag keys."""
        if isinstance(node, str):
            return node
        if isinstance(node, dict):
            texts = []
            for key, value in node.items():
                if key.startswith("@"):
                    continue  # Skip attributes
                text = cls._extract_text(value)
                if text:
                    texts.append(text)
            return " ".join(texts)
        if isinstance(node, list):
            texts = [cls._extract_text(item) for item in node if cls._extract_text(item)]
            return " ".join(texts)
        return ""
