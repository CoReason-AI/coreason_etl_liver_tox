import os
import re
import tarfile
import tempfile
import requests
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import dlt

from coreason_etl_liver_tox.config.settings import settings
from coreason_etl_liver_tox.utils.identity_resolution_policy import EpistemicIdentityResolutionPolicy
from coreason_etl_liver_tox.utils.logger import logger
from coreason_etl_liver_tox.utils.xml_transmutation_policy import EpistemicXmlTransmutationPolicy

@dlt.source(max_table_nesting=0)
def livertox_source() -> Any:
    return livertox_resource()

@dlt.resource(name="coreason_etl_liver_tox_bronze_livertox_raw", write_disposition="merge", primary_key="coreason_id")
def livertox_resource() -> Iterator[list[dict[str, Any]]]:
    # Official NLM Literature Archive HTTPS Endpoint for LiverTox
    base_url = "https://ftp.ncbi.nlm.nih.gov/pub/litarch/29/31/"
    
    logger.info(f"Fetching directory listing from {base_url}")
    response = requests.get(base_url)
    response.raise_for_status()
    
    # Extract the tar.gz filename dynamically
    tar_match = re.search(r'href="([^"]+\.tar\.gz)"', response.text)
    if not tar_match:
        raise ValueError("Could not find the LiverTox .tar.gz archive in the NCBI directory.")
        
    tar_filename = tar_match.group(1)
    download_url = base_url + tar_filename
    
    with tempfile.TemporaryDirectory() as tmpdir:
        local_filepath = os.path.join(tmpdir, "livertox_dump.tar.gz")
        
        logger.info(f"Downloading massive LiverTox archive from {download_url}...")
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(local_filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info("Download complete.")

        logger.info("Extracting and parsing the 9,000+ LiverTox XML monographs...")
        batch = []
        batch_size = settings.ncbi_retmax
        
        with tarfile.open(local_filepath, "r:gz") as tar:
            for member in tar.getmembers():
                if member.name.endswith((".xml", ".nxml")):
                    f = tar.extractfile(member)
                    if f is not None:
                        xml_content = f.read().decode('utf-8', errors='ignore')
                        try:
                            parsed_dict = EpistemicXmlTransmutationPolicy.transmute_xml_to_dict(xml_content)
                            docs = _extract_book_documents(parsed_dict)
                            for doc in docs:
                                doc_id = _extract_uid(doc)
                                blocks = EpistemicXmlTransmutationPolicy.isolate_clinical_text_blocks(doc)
                                
                                batch.append({
                                    "uid": doc_id,
                                    "ingestion_ts": datetime.now(UTC).isoformat(),
                                    "raw_data": doc,
                                    "extracted_blocks": blocks,
                                    "book_id": settings.livertox_book_id,
                                })
                                
                                if len(batch) >= batch_size:
                                    yield EpistemicIdentityResolutionPolicy.apply_deterministic_identity(batch, id_key="uid")
                                    batch = []
                        except Exception as e:
                            logger.error(f"Failed to parse {member.name}: {e}")
                            
        if batch:
            yield EpistemicIdentityResolutionPolicy.apply_deterministic_identity(batch, id_key="uid")

def _extract_book_documents(parsed_dict: dict[str, Any]) -> list[dict[str, Any]]:
    docs = []
    if "book-part" in parsed_dict:
        bp = parsed_dict["book-part"]
        if isinstance(bp, list): docs.extend(bp)
        else: docs.append(bp)
    elif "BookDocumentSet" in parsed_dict:
        bds = parsed_dict["BookDocumentSet"]
        if "BookDocument" in bds:
            bd = bds["BookDocument"]
            if isinstance(bd, list): docs.extend(bd)
            else: docs.append(bd)
    else:
        for val in parsed_dict.values():
            if isinstance(val, list): docs.extend(val)
            elif isinstance(val, dict): docs.append(val)
    return docs

def _extract_uid(document: dict[str, Any]) -> str:
    for id_key in ["article-id", "book-part-id", "book-id", "id"]:
        if id_key in document:
            ids = document[id_key]
            if isinstance(ids, list):
                for i in ids:
                    if isinstance(i, dict) and "#text" in i: return str(i["#text"])
                    if isinstance(i, str): return i
            elif isinstance(ids, dict) and "#text" in ids: return str(ids["#text"])
            elif isinstance(ids, str): return ids
    import hashlib, json
    return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()
