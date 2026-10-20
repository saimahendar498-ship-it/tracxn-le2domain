"""Pipeline prompts preserved from MB1, MB5, MB4/MB8, MB9, and MB12."""

IDENTIFY_PROMPT = (
    "Identify the primary legal entity operating the business in India associated with "
    "'{domain}', including any Indian subsidiary of a foreign company. Provide its entity type, source URL and evidence."
)

RETRY_PROMPT = (
    "Identify the Indian legal entity operating {domain}, including an Indian subsidiary of a foreign parent. "
    "Verify the domain link; don't substitute a brand, parent or same-name firm. Check sources: official site, MCA/GST, "
    "ZaubaCorp, IndiaMART, LinkedIn, trademark/app-store records, Crunchbase and Falconbridge. Classify as Private Limited, "
    "Public Limited, LLP, Partnership, Proprietorship, Trust, Society, Government Entity, Individual or Other if unclear. "
    "Return name, type, evidence, and all sources actually checked with URL and findings. Never list unchecked sources. "
    "If none is identified, say Not Found; no results alone prove absence."
)

VALIDATE_PROMPT = (
    " Validate Indian entity for {domain}. LE={le_name} TYPE={entity_type}. Check official and multiple legal/company sources; "
    "list URLs checked. Verify/correct with evidence; prefer Indian subsidiary over brand/parent. Never invent. Return LE_NAME, "
    "ENTITY_TYPE, SOURCE_URLS, EVIDENCE, STATUS, OUTCOME. STATUS=VALID if correct; CORRECTED if fixed; INCONCLUSIVE if unclear. "
    "OUTCOME=ENTITY_FOUND, NO_REGISTERED_LE or INCONCLUSIVE. NO_REGISTERED_LE needs evidence; no results alone prove absence."
)

EXTRACT_SYSTEM = """
You extract legal entity information.
Return only valid JSON matching the requested structure.
Never fabricate source URLs or claim verification without supporting evidence.
"""

EXTRACT_PROMPT = """
Extract structured information from the provided Google AI Mode SERP API response.
Return valid JSON only:
{
"LE_NAME": "",
"ENTITY_TYPE": "",
"SOURCE_URLS": [],
"EVIDENCE": ""
"STATUS": "FOUND/NOT_FOUND"
}
Rules:
Extract the primary legal entity name and entity type explicitly identified in the response.
Preserve the full legal entity name, including its suffix where stated. Do not invent, correct, or infer missing details.
Extract all relevant source URLs explicitly provided in the response, prioritizing official websites and legal/company registry records. Include only URLs present in the response and remove duplicates.
Summarize the evidence supporting the entity name, entity type, and association with the domain. Include CIN or registration details when provided.
Read the AI overview text, nested markdown, and reference URLs throughout the response. Ignore API metadata and irrelevant follow-up suggestions.
If the response explicitly reports that no entity was found, leave LE_NAME and ENTITY_TYPE empty. If a field is missing, leave it empty; use an empty array when no source URLs are available. Do not interpret a lack of search results as proof that no registered entity exists.
Do not browse the web, introduce external facts, or infer information not supported by the supplied response.
Output JSON only, with no additional commentary or markdown.
Set STATUS to "FOUND" if a legal entity is identified in the response, otherwise "NOT_FOUND" if no identifiable legal entity is provided. If the entity name exists but its type is missing or stated as "Other", set STATUS to "NOT_FOUND".
SERP_RESPONSE: 
"""

VALIDATE_SYSTEM = (
    "You extract legal entity information. "
    "Return only valid JSON matching the requested "
    "structure. Never fabricate source URLs."
)

VALIDATE_EXTRACT_PROMPT = """Extract the final validation result from the Google AI Mode SERP response. Return JSON only:
{"LE_NAME":"","ENTITY_TYPE":"","SOURCE_URLS":[],"EVIDENCE":"","STATUS":"","OUTCOME":""}
Read the AI overview markdown and its nested links/references. Ignore API metadata and follow-up suggestions. Extract the final entity name, type, evidence and all relevant unique source URLs.
STATUS: VALID = original details correct; CORRECTED = details corrected; NOT_FOUND = absence of a registered Indian entity established; INCONCLUSIVE = insufficient evidence.
OUTCOME: ENTITY_FOUND, NO_REGISTERED_LE or INCONCLUSIVE.
Use only information and URLs in the response. Do not browse or invent details. Use empty strings or [] for missing fields.
Input:  
"""
