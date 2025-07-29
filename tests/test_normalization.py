"""Quick test of title normalization edge cases."""

from src.etl.deduplication import normalize_title


def test_normalization_edge_cases():
    """Test some specific normalization scenarios."""

    # Test cases we want to handle well
    test_cases = [
        # SPF often has extra punctuation
        ("Discovery Park: Invasive Plant Removal",
         "discovery park invasive plant removal"),
        ("Lincoln Park - Trail Maintenance & Restoration",
         "lincoln park trail maintenance restoration"),

        # Event types that should group together
        ("Magnolia Park Work Party", "magnolia park work party"),
        ("Magnolia Park: Work-Party", "magnolia park work party"),
        ("Magnolia Park Volunteer Event", "magnolia park volunteer event"),

        # Numbers and special characters
        ("I-90 Trail Clean-up #3", "i 90 trail clean up 3"),
        ("Burke-Gilman Trail @ 70th", "burke gilman trail 70th"),

        # Multiple spaces and mixed case
        ("GOLDEN   GARDENS    restoration", "golden gardens restoration"),

        # Empty/minimal cases
        ("", ""),
        ("A", "a"),
    ]

    for original, expected in test_cases:
        result = normalize_title(original)
        print(f"'{original}' -> '{result}'")
        assert result == expected, f"Expected '{expected}', got '{result}'"

    print("All normalization tests passed!")


if __name__ == "__main__":
    test_normalization_edge_cases()
