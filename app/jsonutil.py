from __future__ import annotations

import json
from typing import Any


def parse_llm_json(content: str) -> dict[str, Any]:
    content = (content or "").strip()
    if not content:
        raise ValueError("The API returned an empty response.")
    if content.startswith("```"):
        content = content.split("\n", 1)[-1]
        if content.endswith("```"):
            content = content[:-3].strip()
        if content.startswith("json"):
            content = content[4:].strip()
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Response is not a JSON object.")
    return json.loads(content[start : end + 1])


def parse_extract_result(content: str) -> dict[str, Any]:
    result = parse_llm_json(content)
    required = {"LE_NAME", "ENTITY_TYPE", "SOURCE_URLS", "EVIDENCE", "STATUS"}
    if not isinstance(result, dict) or not required.issubset(result.keys()):
        raise ValueError("Output JSON has missing keys.")
    for key in ("LE_NAME", "ENTITY_TYPE", "EVIDENCE"):
        if not isinstance(result[key], str):
            raise ValueError(f"{key} must be a string.")
    if not isinstance(result["SOURCE_URLS"], list):
        raise ValueError("SOURCE_URLS must be a list.")
    if not all(isinstance(url, str) for url in result["SOURCE_URLS"]):
        raise ValueError("Every SOURCE_URLS entry must be a string.")
    if result["STATUS"] not in ("FOUND", "NOT_FOUND"):
        raise ValueError("Invalid STATUS value.")
    return {k: result[k] for k in required}


def parse_validate_result(content: str) -> dict[str, Any]:
    result = parse_llm_json(content)
    required = {
        "LE_NAME",
        "ENTITY_TYPE",
        "SOURCE_URLS",
        "EVIDENCE",
        "STATUS",
        "OUTCOME",
    }
    if not isinstance(result, dict) or not required.issubset(result.keys()):
        raise ValueError("JSON response is missing required keys.")
    if not isinstance(result["SOURCE_URLS"], list):
        raise ValueError("SOURCE_URLS must be a list.")
    for field in ("LE_NAME", "ENTITY_TYPE", "EVIDENCE", "STATUS", "OUTCOME"):
        if not isinstance(result[field], str):
            raise ValueError(f"{field} must be a string.")
    return {k: result[k] for k in required}
