import csv
from constants import SECTION_KEYWORDS, CSV_FIELDS
import os
import sys

try:
    import pdfplumber
except ImportError:
    sys.exit("Missing dependency: pip install pdfplumber") 


def get_latest_file(path):
    """
    Scans through files in our volume and returns the path 
    of the last uploaded file using modification time
    """
    files = [f for f in dbutils.fs.ls(path) if not f.isDir()]

    if not files:
        return None
    else:
        last = max(files, key=lambda f: f.modificationTime)
        return last.path[5:]


def extract_all_text(pdf_path):
     """
     Opens the PDF and extracts raw text from every page. 
     Returns dict of { page_number (1-based): [text, [[[table_text]]]] }. 
     Page numbers are assigned by pdfplumber 
     — NOT from printed numbers in the document itself. 
     """
    pages = {}

    # Open pdf using pdfplumber
    with pdfplumber.open(pdf_path) as pdf:

        # Loop through each pdf page, extract text data and table data, place data in list
        for i, page in enumerate(pdf.pages, start=1):
            contents = []
            contents.append(page.extract_text())
            contents.append(page.extract_tables(table_settings={"intersection_x_tolerance": 2, "intersection_y_tolerance": 30, "snap_x_tolerance": 10, "snap_y_tolerance": 8}) or "")
            
            # Add list to pages dictionary
            pages[i] = contents
                  
    return pages


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


#provider is llm service (dbx), model is the endpoint name, host and token default to None
def build_llm_caller(provider, model, api_key=None, 
                     databricks_host=None, databricks_token=None): 
    """ 
    Returns a callable ask(system_prompt, user_prompt) -> str. 
    This is the ONLY part of the script that differs between providers. 
    Everything else is identical regardless of which LLM you use. 
    """ 
    #ensures that the right provider is selected and that the imports are working correctly. inside dbx it always works. 
    if provider == "databricks": 
        try: 
            from databricks.sdk import WorkspaceClient 
            from databricks.sdk.service.serving import ChatMessage, ChatMessageRole 
        except ImportError: 
            sys.exit("pip install databricks-sdk") 

        #workspace client below handles this authenitcation automatically, but again outside of dbx we would have to pass a host/token pair
        # host  = databricks_host  or os.environ.get("DATABRICKS_HOST") 
        # token = databricks_token or os.environ.get("DATABRICKS_TOKEN") 
        # if not host or not token: 
        #     sys.exit("Databricks: provide --databricks-host and --databricks-token") 

        #this creates the actual connection to the workspace were in. this is the object we use to talk to the model serving endpoints
        #client = WorkspaceClient(host=host, token=token) 
        
        client = WorkspaceClient()
        
        #this is a function inside an outer function, pattern called CLOSURE!!!
        #it automatically has access to client and model through inheritance of the variables, hence you don't have to pass them around
        def ask(system_prompt, user_prompt): 
            #actual api call. query hits the endpoint, messages has the system prompt (instructions) and user prompt (pdf text)
            response = client.serving_endpoints.query( 
                name="databricks-meta-llama-3-3-70b-instruct",
                messages=[ 
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt), 
                    ChatMessage(role=ChatMessageRole.USER,   content=user_prompt), 
                ], 
                max_tokens=4096, 
            ) 
            return response.choices[0].message.content

    else: 
        sys.exit(f"Unknown provider '{provider}'. Choose: databricks") 

    return ask 


def write_unified_csv(all_rows, output_path):
     pass



