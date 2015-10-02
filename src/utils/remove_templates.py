def remove_templates(text):
    """Remove all text contained between matched pairs of '{{' and '}}'.

    Args:
        text (str): Full text of a Wikipedia article as a single string.

    Returns:
        str: A copy of the full text with all templates removed.

    """
    good_char_list = []

    prev_char = None
    next_char = None

    depth = 0
    open_pos = None
    close_pos = None

    for pos,char in enumerate(text):
        try:
            next_char = text[pos+1]
        except IndexError:
            next_char = None

        if char == '{' and next_char == '{':
            depth += 1
        elif char == '}' and prev_char == '}':
            depth -= 1
        # Add characters if we are at depth 0
        elif depth == 0:
            good_char_list.append(char)

        prev_char = char

    return ''.join(good_char_list)
