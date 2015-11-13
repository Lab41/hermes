import re
def clean_categories(text):
    """Replace Wikipedia category links with the name of the category in the
    text of an article.

    Text like "[[Category:Foo]]" will be replaced with "Foo". Sorting hints are
    thrown away during this cleaning, so text like "[[Category:Bar|Sorting
    hint]]" will be replaced with "Bar".

    Args:
        text (str): The full text of a Wikipedia article in one string.

    Returns:
        str: The full text with Category links replaced.

    """
    # Since Regexes are unreadable, let me explain:
    #
    # "\[\[Category:([^\[\]|]*)[^\]]*\]\]" consists of several parts:
    #
    #    \[\[ matches "[["
    #
    #    Category: matches the text "Category:"
    #
    #    (...) is a capture group meaning roughly "the expression inside this
    #    group is a block that I want to extract"
    #
    #    [^...] is a negated set which means "do not match any characters in
    #    this set".
    #
    #    \[, \], and | match "[", "]", and "|" in the text respectively
    #
    #    * means "match zero or more of the preceding regex defined items"
    #
    #    [^\]]* means match any character but a ']'
    #
    #    \]\] matches "]]"
    #
    #  So putting it all together, the regex does this:
    #
    #    Finds "[[" followed by "Category:" and then matches any number
    #    (including zero) characters after that that are not the excluded
    #    characters "[", "]", or "|". These matched characters are saved. When
    #    it hits an excluded character, it begins matching any characters
    #    except "[". It throws these matched characters away. It terminates
    #    when it finds "]]".
    #
    return re.sub(r'\[\[Category:([^\[\]|]*)[^\]]*\]\]', r'\1', text)
