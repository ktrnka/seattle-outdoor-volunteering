from openai import OpenAI
import os


class MissingGithubToken(ValueError):
    """Raised when GITHUB_TOKEN environment variable is missing or empty."""

    def __init__(self, message: str = "GITHUB_TOKEN environment variable is required for LLM functionality."):
        super().__init__(message)


def get_client() -> OpenAI:
    try:
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
