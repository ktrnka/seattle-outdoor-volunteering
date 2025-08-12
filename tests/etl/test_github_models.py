"""Tests for GitHub Models catalog parsing."""

import json
from pathlib import Path

import pytest

from src.llm.llm import GitHubModel, parse_github_models_catalog


def test_parse_github_models_catalog():
    """Test parsing the GitHub Models catalog JSON."""
    # Load test data
    test_data_path = Path(__file__).parent / "data" / "github_models_catalog.json"
    with open(test_data_path) as f:
        catalog_data = json.load(f)
    
    # Parse the catalog
    models = parse_github_models_catalog(catalog_data)
    
    # Basic validation
    assert len(models) > 0
    assert all(isinstance(model, GitHubModel) for model in models)
    
    # Check for our target model
    model_ids = [model.id for model in models]
    assert "openai/gpt-4.1" in model_ids
    
    # Find and validate the gpt-4.1 model
    gpt41_model = next(model for model in models if model.id == "openai/gpt-4.1")
    assert gpt41_model.name == "OpenAI GPT-4.1"
    assert gpt41_model.publisher == "OpenAI"
    assert "tool-calling" in gpt41_model.capabilities
    assert gpt41_model.limits.max_input_tokens == 1048576
    assert gpt41_model.limits.max_output_tokens == 32768


def test_github_model_structure():
    """Test the GitHubModel Pydantic model structure."""
    # Test with our known gpt-4.1 model data
    model_data = {
        "id": "openai/gpt-4.1",
        "name": "OpenAI GPT-4.1",
        "publisher": "OpenAI",
        "summary": "gpt-4.1 outperforms gpt-4o across the board",
        "rate_limit_tier": "high",
        "supported_input_modalities": ["text", "image"],
        "supported_output_modalities": ["text"],
        "tags": ["multipurpose", "multilingual", "multimodal"],
        "registry": "azure-openai",
        "version": "2025-04-14",
        "capabilities": ["streaming", "tool-calling"],
        "limits": {
            "max_input_tokens": 1048576,
            "max_output_tokens": 32768
        },
        "html_url": "https://github.com/marketplace/models/azure-openai/gpt-4-1"
    }
    
    model = GitHubModel(**model_data)
    assert model.id == "openai/gpt-4.1"
    assert model.name == "OpenAI GPT-4.1"
    assert model.capabilities == ["streaming", "tool-calling"]
    assert model.limits.max_input_tokens == 1048576


def test_list_available_models():
    """Test that we can list available model IDs."""
    test_data_path = Path(__file__).parent / "data" / "github_models_catalog.json"
    with open(test_data_path) as f:
        catalog_data = json.load(f)
    
    models = parse_github_models_catalog(catalog_data)
    model_ids = [model.id for model in models]
    
    # Should contain various model families
    assert any(model_id.startswith("openai/") for model_id in model_ids)
    assert any(model_id.startswith("meta/") for model_id in model_ids)
    assert any(model_id.startswith("microsoft/") for model_id in model_ids)
    
    # Should contain our specific models
    assert "openai/gpt-4.1" in model_ids
    assert "openai/gpt-4o" in model_ids
