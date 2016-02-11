from copy import deepcopy
import ast
import numpy as np


class git_vectorize():

    def __init__(self, git_rdd, user_vector_type, content_vector_type, spark_context, **support_files):
        """Set up the Git vectorizer.

        Args:
            git_rdd (rdd): The raw git data. Each "row" is as follows:
                Row(author, author_mail, author_time, author_timezone, comment,
                commit_id, committer, committer_mail, committer_time,
                committer_timezone, filename, line, line_num, repo_name)
            user_vector_type (str): The type of user vector desired. One of
            'any_interact', 'num_interact', or None.
            content_vector_type: The type of content vector desired. One of
                'py2vec' or None.
            support_files: Only one support file is used for this class:
                model: A dictionary mapping words to numpy vectors.

        Raise:
            ValueError: if the model is missing

        """
        self.git_rdd = git_rdd
        self.sc = spark_context
        self.content_vector_type = content_vector_type
        self.user_vector_type = user_vector_type

        # Set up the support files
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}

        # Set up the author map
        self.__set_author_map()
        # Map each line in the file to an author_id
        self.__set_lines_to_authorid()  # ((repo, file, line_num), author_id)

        # Map lines to functions
        self.__set_lines_to_functions()  # ((repo, file, line_num), function)
        self.__set_function_map()

        # Join the authors to the functions they use
        self.__join_authors_to_functions()  # (author_id, function_id)

    def __set_lines_to_functions(self):
        loaded_data = code_lines.map(
            lambda (
                author,
                author_mail,
                author_time,
                author_timezone,
                comment,
                commit_id,
                committer,
                committer_mail,
                committer_time,
                committer_timezone,
                filename,
                line,
                line_num,
                repo_name,
            ):
            ((repo_name, filename), ((author, author_mail), line_num, line))
        )
        # Group data by file so that we can reconstruct the file
        grouped_lines = loaded_data.groupByKey()
        # Reconstruct the files from their lines so we can run them through the lexer
        reconstructed_files = grouped_lines.map(lambda (key, lines): (key, lines_to_file(lines)))
        # Make a map of files to functions
        file_to_functions = reconstructed_files.flatMap(lambda row: run_lexer(row))
        # Map lines to functions
        self.lines_to_functions = file_to_functions.map(
                lambda ((repo, file), (line_num, cont_type, cont)):
                ((repo, file, line_num), clean_names(cont))
            )

    def __join_authors_to_functions(self):
        joined_authors_and_functions = self.lines_to_authorid.join(self.lines_to_functions)

        # We must deepcopy the map so that Spark can pack up the dictionary and
        # ship it around our cluster. If we do not deepcopy, Spark tries to
        # pack up this whole class which fails because their are RDDs stored in
        # the class.
        function_map = sc.broadcast(deepcopy(self.function_map))

        self.authorid_functionid = joined_authors_and_functions\
            .map(
                lambda (line, (author_id, function)): (author_id, function_map.value[function])
            )

    def __set_function_map(self):
        functions = self.lines_to_functions\
            .map(lambda (line, function): function)\
            .distinct()

        self.function_map = {k: v for v, k in enumerate(functions.collect())}

    def __set_author_map(self):
        authors = self.git_rdd.map(
            lambda (
                author,
                author_mail,
                author_time,
                author_timezone,
                comment,
                commit_id,
                committer,
                committer_mail,
                committer_time,
                committer_timezone,
                filename,
                line,
                line_num,
                repo_name,
            ):
            (author, author_mail)
        ).distinct()

        self.author_map = {k: v for v, k in enumerate(authors.collect())}

    def __set_lines_to_authorid(self):
        # We must deepcopy the map so that Spark can pack up the dictionary and
        # ship it around our cluster. If we do not deepcopy, Spark tries to
        # pack up this whole class which fails because their are RDDs stored in
        # the class.
        author_map = sc.broadcast(deepcopy(self.author_map))

        self.lines_to_authorid = code_lines.map(
            lambda (
                author,
                author_mail,
                author_time,
                author_timezone,
                comment,
                commit_id,
                committer,
                committer_mail,
                committer_time,
                committer_timezone,
                filename,
                line,
                line_num,
                repo_name,
            ):
            ((repo_name, filename, line_num), author_map.value[(author, author_mail)])
        )

    def get_user_vector(self):
        """Produce an RDD containing tuples of the form (user, item, rating).

        There are two options when producing these user vectors:

            any_interact: 1 if the user has used a function, excluded
                otherwise.
            num_iteract: the rating is the number of times a user used a
                function.

        """
        # Starts as (author_id, function_id) with duplicates
        a_f = self.authorid_functionid

        # Any interaction leads to a rating of 1
        if self.user_vector_type == "any_interact":
            user_info = a_f\
                .distinct()\
                .map(lambda (user, item): (user, item, 1))

            return user_info

        # Count the number of interactions, that is, the number of times the
        # user has used a function
        if self.user_vector_type == "num_interact":
            user_info = a_f\
                .map(lambda (user, item): ((user, item), 1))\
                .reduceByKey(lambda v1, v2: v1+v2)\
                .map(lambda ((user, item), count): (user, item, count))
            return user_info

        # Nothing to do!
        elif self.user_vector_type is None or self.user_vector_type == 'none':
            return None

        # Error state
        else:
            print "Please choose a user_vector_type from 'any_interact', 'num_interact', or None"
            return None

    def get_content_vector(self):
        """Produce an RDD containing tuples of the form (item, content_vector).

        There is one method of producing content vectors:

            py2vec: Uses a Word2Vec model to classify functions.

        """
        if self.content_vector_type == "py2vec":
            # Get the word model passed in through the constructor
            try:
                word_model = self.support_files["model"]
            except KeyError:
                raise ValueError

            # We must deepcopy the map so that Spark can pack up the dictionary
            # and ship it around our cluster. If we do not deepcopy, Spark
            # tries to pack up this whole class which fails because their are
            # RDDs stored in the class.
            model = sc.broadcast(deepcopy(word_model))
            function_map = sc.broadcast(deepcopy(self.function_map))

            # Starts as ((repo, file, line_num), function)
            functions = self.sc.parallelize(self.function_map.keys())
            content_vector = functions\
                .map(lambda function: (function_map.value[function], function_to_vector(function, model.value)))\
                .filter(lambda (functionid, vector): vector is not None)\
                .filter(lambda (functionid, vector): vector.any())

            return content_vector

        # Nothing to do!
        elif self.content_vector_type is None or self.content_vector_type == 'none':
            return None

        # Error state
        else:
            print "Please choose a content_vector_type from 'py2vec' or None"
            return None


class Lexer(ast.NodeVisitor):
    """Parse a node from a AST and return a tuple of the content.

    The returned tuple is of the form:

        (line_number, node_type, name)

    node_type can be either "Import" or "Call".

    """

    def visit_Import(self, node):
        """Called for "import library" statements."""
        items = []
        for item in node.names:
            items.append((node.lineno, "Import", item.name))
        self.generic_visit(node)
        return items

    def visit_ImportFrom(self, node):
        """Called for "from library import object" statements."""
        self.generic_visit(node)
        return [(node.lineno, "Import", node.module)]

    def visit_Call(self, node):
        """Called for function and method calls."""
        id = None
        # Some nodes have their name in the function object
        try:
            id = node.func.id
        except AttributeError:
            pass
        # Others (those called as methods, or with a library name leading) have
        # the name in the attr block
        try:
            id = node.func.value.id + '.' + node.func.attr
        except AttributeError:
            pass

        self.generic_visit(node)

        if id:
            return [(node.lineno, "Call", id)]


def run_lexer(row):
    """Run the lexer over the text of a Python file.

    Args:
        row: A row from an RDD of the form ((repo, file), file_text)

    Returns:
        list of tuples: each tuples is of the form (object_type, object_name).
            Returns an empty list on parsing failure.
    """
    uniq_id, content = row
    new_content = []
    # There is a bug in Python when the first line is a coding statement:
    # https://bugs.python.org/issue22221
    for line in content.splitlines():
        if not (line.startswith("#") and "coding:" in line):
            new_content.append(line)

    try:  # Sometimes Python3 files sneak in that can not be parsed
        tree = ast.parse('\n'.join(new_content))
    except Exception as e:
        return []

    output = []
    for node in ast.walk(tree):
        ret = Lexer().visit(node)
        if ret:
            for item in ret:
                output.append((uniq_id, item))

    if output:
        return output
    else:
        return []


def clean_names(function):
    """Clean function names.

    Args:
        function (str): a function name.

    Returns:
        str: A string with all characters lowered and only the last word if
            there are multiple period joined words.

    """
    if function is not None:
        out = function.lower()
        out = out.split('.')[-1]
        return out


def function_to_vector(function, model):
    """Assigns a vector to a function.

    Args:
        function (str): the function name.
        model (dict): A mapping that words as follows:
            model[function] == vector

    Returns:
        vector: A numpy array, or None if the function was not found in the
            model.
    """
    try:
        return model[function]
    except:
        return None


def lines_to_file(lines):
    """Construct a file from the lines that make it up.

    Args:
        vals: an iterator over rows of the following form:
            (author, line_num, line_text)

    Returns:
        str: The full text of the file
    """
    output = ""
    for (author, line_num, line) in sorted(lines):
        output += line + '\n'

    return output
