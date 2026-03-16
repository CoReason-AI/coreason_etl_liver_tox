import re


def test_dili_likelihood_score_regex() -> None:
    """Test the mandatory regex used in the dbt model."""
    regex = r"Likelihood score:\s*([A-EX](?:\[[^\]]+\]|\*)?)"

    test_cases = [
        ("Likelihood score: A", "A"),
        ("Likelihood score: B[HD]", "B[HD]"),
        ("Likelihood score: E*", "E*"),
        ("Likelihood score:    C", "C"),
        ("Likelihood score: D", "D"),
        ("Likelihood score: X", "X"),
        ("Likelihood score: A[something]", "A[something]"),
        ("Likelihood score: B[HD]*", "B[HD]"),  # Based on regex, should probably match B[HD]
        ("Some other text Likelihood score: A and more", "A"),
    ]

    for text, expected in test_cases:
        match = re.search(regex, text)
        assert match is not None
        assert match.group(1) == expected

    negative_cases = [
        "Likelihood score: Z",
        "Likelihood score: 1",
        "Likelihood score: ",
    ]

    for text in negative_cases:
        match = re.search(regex, text)
        assert match is None
