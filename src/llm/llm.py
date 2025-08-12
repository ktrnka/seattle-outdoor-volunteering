import os
import traceback
from typing import List, Optional

import requests
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, HttpUrl


class ModelLimits(BaseModel):
    """Model limits from GitHub Models catalog."""

    max_input_tokens: int
    max_output_tokens: Optional[int] = None


class GitHubModel(BaseModel):
    """A model from the GitHub Models catalog."""

    id: str
    name: str
    publisher: str
    summary: str
    rate_limit_tier: str
    supported_input_modalities: List[str]
    supported_output_modalities: List[str]
    tags: List[str]
    registry: str
    version: str
    capabilities: List[str]
    limits: ModelLimits
    html_url: HttpUrl


def parse_github_models_catalog(catalog_data: List[dict]) -> List[GitHubModel]:
    """Parse the GitHub Models catalog JSON response into Pydantic models."""
    models = []
    for model_data in catalog_data:
        try:
            model = GitHubModel(**model_data)
            models.append(model)
        except Exception as e:
            print(f"[WARNING] Failed to parse model {model_data.get('id', 'unknown')}: {e}")
    return models


def fetch_github_models_catalog() -> List[GitHubModel]:
    """Fetch and parse the GitHub Models catalog."""
    try:
        token = os.environ["GITHUB_TOKEN"]
    except KeyError:
        raise MissingGithubToken()

    if not token:
        raise MissingGithubToken("GITHUB_TOKEN environment variable is empty.")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    response = requests.get("https://models.github.ai/catalog/models", headers=headers)
    response.raise_for_status()

    catalog_data = response.json()
    return parse_github_models_catalog(catalog_data)


class MissingGithubToken(ValueError):
    """Raised when GITHUB_TOKEN environment variable is missing or empty."""

    def __init__(self, message: str = "GITHUB_TOKEN environment variable is required for LLM functionality."):
        super().__init__(message)


def get_client() -> OpenAI:
    try:
        load_dotenv()
        token = os.environ["GITHUB_TOKEN"]
    except KeyError:
        raise MissingGithubToken()

    if not token:
        raise MissingGithubToken("GITHUB_TOKEN environment variable is empty.")

    endpoint = "https://models.github.ai/inference"

    client = OpenAI(
        base_url=endpoint,
        api_key=token,
    )
    return client


def debug_list_catalog(target_model: str):
    # Debug: List available models using GitHub Models catalog API
    print("[DEBUG] Attempting to list available models via GitHub Models catalog...")
    try:
        models = fetch_github_models_catalog()
        available_model_ids = [model.id for model in models]
        print(f"[DEBUG] Available models ({len(available_model_ids)}): {sorted(available_model_ids)}")

        # Check if our target model is available
        if target_model not in available_model_ids:
            print(f"[WARNING] Target model '{target_model}' not found in available models")
            # Look for similar models
            gpt4_models = [m for m in available_model_ids if "gpt-4" in m.lower()]
            openai_models = [m for m in available_model_ids if m.startswith("openai/")]
            print(f"[DEBUG] Available GPT-4 models: {sorted(gpt4_models)}")
            print(f"[DEBUG] Available OpenAI models: {sorted(openai_models)}")
        else:
            print(f"[DEBUG] Target model '{target_model}' found in available models")
            # Show details about the target model
            target_model_info = next(model for model in models if model.id == target_model)
            print(f"[DEBUG] Target model details: {target_model_info.name} by {target_model_info.publisher}")
            print(f"[DEBUG] Capabilities: {target_model_info.capabilities}")
            print(f"[DEBUG] Input tokens limit: {target_model_info.limits.max_input_tokens}")
            print(f"[DEBUG] Output tokens limit: {target_model_info.limits.max_output_tokens}")
    except Exception as e:
        print(f"[ERROR] Failed to fetch GitHub Models catalog: {e}")
        print(f"[ERROR] Exception type: {type(e)}")
        print("[ERROR] Full traceback:")
        traceback.print_exc()
