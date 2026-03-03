ROLES = [
    "Associate", "Senior Associate", "Consultant", "Senior Consultant","Manager", "Senior Manager", "Associate Director", "Director"
    ] 


DUAL_PATH_ROLES = {"Manager", "Senior Manager", "Associate Director"}


SECTION_KEYWORDS = {
    "annex4": ["annex 4", "data and ai adoption"],
    "annex3": ["annex 3", "managed revenue"],
    "annex2b": ["annex 2b", "technical skills progression"],
    "annex2a": ["annex 2a", "consulting skills progression"],
    "annex1": ["annex 1", "proficiency levels"],
    "role_profiles": ["role profile"]
    } 


OUTPUT_FILE = "kubrick_competency_framework.csv"


CSV_FIELDS = ["section", "role", "path", "field_name", "field_value", "field_context"] 


COMMON_SYSTEM_INTRO = (
    "You are a structured data extractor working with HR competency framework documents."
    "Return ONLY a valid JSON array. No markdown, no code fences, no explanation. "
    "Each element must be a flat JSON object with exactly the keys specified."
    ) 