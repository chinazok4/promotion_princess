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
    pages = {}
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            contents = []
            
            # find table bounding boxes on this page
            tables = page.find_tables(table_settings={
                "intersection_x_tolerance": 2,
                "intersection_y_tolerance": 30,
                "snap_x_tolerance": 10,
                "snap_y_tolerance": 8
            })
            # filter out any characters that fall inside a table bounding box
            if tables:
                table_bboxes = [t.bbox for t in tables]
                def not_in_table(obj):
                    for bbox in table_bboxes:
                        if (obj["x0"] >= bbox[0] and obj["x1"] <= bbox[2] and
                            obj["top"] >= bbox[1] and obj["bottom"] <= bbox[3]):
                            return False
                    return True
                filtered_page = page.filter(not_in_table)
                text = filtered_page.extract_text() or ""
            else:
                text = page.extract_text() or ""
            # extract tables separately
            extracted_tables = page.extract_tables(table_settings={
                "intersection_x_tolerance": 2,
                "intersection_y_tolerance": 30,
                "snap_x_tolerance": 10,
                "snap_y_tolerance": 8
            }) or ""

            contents.append(text)
            contents.append(extracted_tables)
            pages[i] = contents
    return pages


def find_section_pages(pages):
    """
    Identifies which pages belong to which section of the competency framework.
    
    Input: dict of {page_num: [text_string, [tables]]} (V2 format)
           also handles {page_num: text_string} (V1 format) for backwards compatibility
    
    Returns: dict {section_name: [page_numbers]}
    Sections with no detected anchor return an empty list.
    """

    section_order = ["role_profiles", "annex1", "annex2a", "annex2b", "annex3", "annex4"]
    sorted_pages = sorted(pages.items(), key=lambda x: x[0])
    #{section_name: first_page_number_where_keyword_found}
    anchors = {}

    SECTION_KEYWORDS = {"role_profiles": ["role profile"], "annex1": ["annex 1: competency framework"], "annex2a": ["annex 2a: consulting skills progression"], "annex2b": ["annex 2b: technical skills progression"], "annex3": ["annex 3: managed revenue"], "annex4": ["annex 4: data and ai adoption"]}

    for sec in section_order:
        keywords = SECTION_KEYWORDS[sec]
        for page_num, content in sorted_pages:

            # handles where content is a list where [0] is text string, [1] is tables
            # edge case: content is a plain string
            # edge case: pages that are tables only and have an empty text string
            if isinstance(content, list):
                text = content[0] if content and isinstance(content[0], str) else ""
            else:
                text = content
            text_lower = text.lower()

            # record keywords pages, continue until new keyword page foud
            if any(kw in text_lower for kw in keywords):
                anchors.setdefault(sec, page_num)
                break  

    #build section, start tuples sorted so we can determine boundaries
    anchored_sections = [(sec, anchors[sec]) for sec in section_order if sec in anchors]
    anchored_sections.sort(key=lambda x: x[1])

    #initialize output dict and all page numbers
    section_pages = {sec: [] for sec in section_order}
    all_page_nums = [p for p, _ in sorted_pages]

    for i, (sec, start_page) in enumerate(anchored_sections):
        
        # the section runs from its start page up to (but not including) the next section's start page
        # if this is the last section, it runs to the end of the document
        if i + 1 < len(anchored_sections):
            next_start = anchored_sections[i + 1][1]
        else:
            next_start = float("inf")

        # assign all page numbers that fall within this section's boundary
        section_pages[sec] = [p for p in all_page_nums if start_page <= p < next_start]

    return section_pages

#--- THIS NEEDS TO BE DONE === 
def pages_to_text(pages, page_nums):
    """
    Builds a clean string from the specified pages to send to the LLM.
    
    Strategy:
    - Always include text (content[0]) — captures prose like core objectives
    - If tables exist (content[1] is a list) — also append serialized tables
    - Both included per page since they capture different content:
      text has prose/objectives (table regions already stripped in extract_all_text),
      tables have clean structured KPI/competency data
    """
    chunks = []

    for p in page_nums:
        if p not in pages:
            continue

        content = pages[p]
        text = content[0] if isinstance(content[0], str) else ""
        tables = content[1] if len(content) > 1 else ""

        page_parts = []

        # always include text if it has content
        if text.strip():
            page_parts.append(text.strip())

        # if tables exist, serialize and append them
        if isinstance(tables, list) and len(tables) > 0:
            for table in tables:
                table_lines = []
                for row in table:
                    cells = [str(cell).strip() for cell in row if cell is not None]
                    if any(cells):
                        table_lines.append(" | ".join(cells))
                if table_lines:
                    page_parts.append("TABLE:\n" + "\n".join(table_lines))

        if page_parts:
            chunks.append(f"--- PAGE {p} ---\n" + "\n\n".join(page_parts))

    return "\n\n".join(chunks)


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

#--- THIS NEEDS TO BE DONE --- 
def write_unified_csv(all_rows, output_path):
     pass



