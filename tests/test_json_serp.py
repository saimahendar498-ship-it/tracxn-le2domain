from app.jsonutil import parse_extract_result, parse_validate_result
from app.serp import extract_answer, serp_for_llm


def test_extract_answer_sources_and_markdown():
    task = {
        "result": [
            {
                "items": [
                    {
                        "text": "Hello",
                        "markdown": "# Hello",
                        "references": [
                            {"title": "A", "url": "https://a.example"},
                            {"title": "A", "url": "https://a.example"},
                        ],
                        "items": [{"text": "Nested"}],
                    }
                ]
            }
        ]
    }
    full, md = extract_answer(task)
    assert "Hello" in full
    assert "Nested" in full
    assert "SOURCES:" in full
    assert full.count("https://a.example") == 1
    assert md == "# Hello"
    blob = serp_for_llm(full, md)
    assert "MARKDOWN:" in blob


def test_parse_fenced_json_and_required_keys():
    raw = """```json
    {"LE_NAME":"X Pvt Ltd","ENTITY_TYPE":"Private Limited","SOURCE_URLS":["https://x"],"EVIDENCE":"e","STATUS":"FOUND","EXTRA":1}
    ```"""
    parsed = parse_extract_result(raw)
    assert parsed["LE_NAME"] == "X Pvt Ltd"
    assert "EXTRA" not in parsed

    raw2 = '{"LE_NAME":"X","ENTITY_TYPE":"Private Limited","SOURCE_URLS":[],"EVIDENCE":"e","STATUS":"VALID","OUTCOME":"ENTITY_FOUND"}'
    v = parse_validate_result(raw2)
    assert v["OUTCOME"] == "ENTITY_FOUND"
