def remove_templates(text):
    """Remove all text contained between '{{' and '}}', even in the case of
    nested templates.

    Args:
        text (str): Full text of a Wikipedia article as a single string.

    Returns:
        str: The full text with all templates removed.

    """
    start_char = 0
    while '{{' in text:
        depth = 0
        prev_char = None
        open_pos = None
        close_pos = None

        for pos in xrange(start_char, len(text)):
            char = text[pos]
            # Open Marker
            if char == '{' and prev_char == '{':
                if depth == 0:
                    open_pos = pos-1
                    # When we scan the string again after removing the chunk
                    # that starts here, we know all text before is template
                    # free, so we mark this position for the next while
                    # iteration
                    start_char = open_pos
                depth += 1
            # Close Marker
            elif char == '}' and prev_char == '}':
                depth -= 1
                if depth == 0:
                    close_pos = pos
                    # Remove all text between the open and close markers
                    text = text[:open_pos] + text[close_pos+1:]
                    break
            prev_char = char

    return text
