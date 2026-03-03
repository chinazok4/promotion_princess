import json
import pytest
from helpers import build_llm_caller

#tests for build llm caller. broken into 3 parts.
#this one ensures thatthe function builds and returns a callable object (in this case a function)
def test_returns_callable():
    ask = build_llm_caller(
        provider="databricks",
        model="databricks-meta-llama-3-3-70b-instruct"
    )
    assert callable(ask)

#this one ensures that the model responds
def test_model_responds():
    ask = build_llm_caller(
        provider="databricks",
        model="databricks-meta-llama-3-3-70b-instruct"
    )
    result = ask(
        "You are a structured data extractor. Return only valid JSON.",
        'Return this exact JSON: [{"test": "working"}]'
    )
    assert result is not None
    assert len(result) > 0

#this one ensures that when there is an output that it is stored in a valid json format
def test_returns_valid_json():
    ask = build_llm_caller(
        provider="databricks",
        model="databricks-meta-llama-3-3-70b-instruct"
    )
    result = ask(
        "You are a structured data extractor. Return only valid JSON.",
        'Return this exact JSON: [{"test": "working"}]'
    )
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert parsed[0]["test"] == "working"


def test_invalid_provider_exits():
    with pytest.raises(SystemExit):
        build_llm_caller(provider="openai", model="gpt-4o")


def test_extract_all_text():
    pass


def test_find_section_pages():
    pass


def test_pages_to_text():
    pass