from __future__ import annotations

from typing import Any


def _text_of(el: dict[str, Any]) -> str:
    return (el.get("text") or el.get("markdown") or "").strip()


def _refs_of(obj: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for ref in obj.get("references") or []:
        title = ref.get("title") or ref.get("source") or ref.get("domain") or ""
        url = ref.get("url") or ""
        if title or url:
            out.append(f"- {title} {url}".strip())
    return out


def extract_answer(task: dict[str, Any]) -> tuple[str, str]:
    """Return (full_text, markdown) from a DataForSEO AI Mode task payload."""
    full_parts: list[str] = []
    md_parts: list[str] = []
    refs: list[str] = []
    for res in task.get("result") or []:
        for item in res.get("items") or []:
            md = item.get("markdown")
            if not md:
                sub_md = [
                    (s.get("markdown") or s.get("text") or "")
                    for s in item.get("items") or []
                ]
                md = "\n".join(x for x in sub_md if x)
            if md:
                md_parts.append(md.strip())

            txt = (item.get("text") or "").strip()
            sub_txt = [_text_of(s) for s in item.get("items") or []]
            sub_txt = [x for x in sub_txt if x]
            if txt:
                full_parts.append(txt)
            if sub_txt:
                full_parts.append("\n".join(sub_txt))
            if not txt and not sub_txt and md:
                full_parts.append(md.strip())

            refs += _refs_of(item)
            for sub in item.get("items") or []:
                refs += _refs_of(sub)

    full = "\n\n".join(full_parts).strip()
    if refs:
        full += "\n\nSOURCES:\n" + "\n".join(dict.fromkeys(refs))
    markdown = "\n\n".join(md_parts).strip()
    if not full and not markdown:
        return "NO_AI_MODE_RESULT", ""
    return full, markdown


def serp_for_llm(full_text: str, markdown: str) -> str:
    if markdown and markdown not in (full_text or ""):
        return f"{full_text}\n\nMARKDOWN:\n{markdown}".strip()
    return full_text or ""
