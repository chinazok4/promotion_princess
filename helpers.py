import csv
from constants import SECTION_KEYWORDS, CSV_FIELDS

try:
    import pdfplumber
except ImportError:
    sys.exit("Missing dependency: pip install pdfplumber") 


def extract_all_text(pdf_path):
     """
     Opens the PDF and extracts raw text from every page. 
     Returns dict of { page_number (1-based): text }. 
     Page numbers are assigned by pdfplumber 
     — NOT from printed numbers in the document itself. 
     """
    pass


def find_section_pages(pages):
     """
     Scans each page for section keywords (case-insensitive substring match).
     'role profile' matches both 'Role Profile' and 'Role Profiles'.
     Annexes are checked first (more specific) so a page isn't double-counted.
     Returns dict of { section_name: [sorted page numbers] }.
     """
    pass


def pages_to_text(pages, page_nums):
    pass


def build_llm_caller(provider, model, api_key=None,  databricks_host=None, databricks_token=None):
     """
     Returns a callable ask(system_prompt, user_prompt) -> str.
     This is the ONLY part of the script that differs between providers.
     Everything else is identical regardless of which LLM you use.
     """ 
    pass


def write_unified_csv(all_rows, output_path):
    pass



