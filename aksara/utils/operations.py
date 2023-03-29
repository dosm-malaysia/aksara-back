"""Performs post-operation on data formatting."""


def perform_operation(data, operation):
    """
    Performs post-operation on data formatting.
    """
    if operation == "_REVERSE_":
        return list(reversed(data))

    return data
