#!/usr/bin/env python

def clean_links(text):
    """Remove brackets around a wikilink, keeping the label instead of the page
    if it exists.

    "[[foobar]]" will become "foobar", but "[[foobar|code words]]" will return
    "code words".

    Args:
        text (str): Full text of a Wikipedia article as a single string.

    Returns:
        str: A copy of the full text with all wikilinks cleaned.

    """
    good_char_list = []
    next_char = None
    skip = 0

    for pos,char in enumerate(text):
        try:
            next_char = text[pos+1]
        except IndexError:
            next_char = None

        # Skip the character
        if skip:
            skip -= 1
            continue

        # Otherwise check if we have found a link
        if char == '[' and next_char == '[':
            skip = 1
            # Check if we are in a comment with
            pipe_pos = text.find('|', pos)
            if pipe_pos == -1:
                continue
            end_pos = text.find(']]', pos)
            if pipe_pos < end_pos:
                skip = pipe_pos - pos

        elif char == ']' and next_char == ']':
            skip = 1

        # Otherwise just append the character
        else:
            good_char_list.append(char)

    return ''.join(good_char_list)
