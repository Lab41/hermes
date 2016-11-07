import numpy as np
import string
from copy import deepcopy
import ast
import json
from pyspark.sql.types import *


class kaggle_vectorize():

    def __init__(self, user_interactions, content, user_vector_type, content_vector_type, sqlCtx, spark_context, **support_files):
        """Set up the Kaggle Vectorizer.

        Args:
            user_interactions (rdd): The raw data of users interactions with
                the kaggle scripts (known as kernals on the website)
                This is the Kaggle meta-data Scripts.csv file
            content (rdd): The raw data about the sript content.
                This is the Kaggle meta-data ScriptsVersions.csv file
            user_vector_type (str): The type of user vector desired. One of
                'forked_interact', 'any_interact', 'num_interact', or None.
                The option 'forked_interact' currently has a different object id, so would have issues in a content based setting
            content_vector_type: The type of content vector desired. One of
                'py2vec' or None.
            support_files: Only one support file is used for this class:
                 model: A dictionary mapping words to numpy vectors.

        """

        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type

        self.user_interactions = user_interactions
        self.content = content
        self.sqlCtx = sqlCtx

        # If no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


        self.sc = spark_context


        # Map lines to functions
        self.__set_lines_to_functions()  # (script_id, function)
        self.__set_function_map() # (function, function_id)

        # Get the script authors
        self._set_scripts_to_authors() # (script_id, author_id)

        # Join the authors to the functions they use
        self.__join_authors_to_functions()  # (author_id, function_id)


    def get_elems(self):
        return [self.lines_to_functions, self.authorid_functionid, self.function_map, self.author_data]

    def __set_lines_to_functions(self):
        # For our purposes we really only care about Python Scripts or IPython Notebooks so filter these out
        python_code = self.content.rdd.filter(lambda row: row.ScriptLanguageId in ('2', '8', '9'))

        loaded_data = python_code.map(
            lambda (
                DateCreated,
                Id_,
                IsChange,
                LinesChangedFromFork,
                LinesChangedFromPrevious,
                LinesDeletedFromFork,
                LinesDeletedFromPrevious,
                LinesInsertedFromFork,
                LinesInsertedFromPrevious,
                LinesUnchangedFromFork,
                LinesUnchangedFromPrevious,
                ScriptContent,
                ScriptId,
                ScriptLanguageId,
                TemplateScriptVersionId,
                Title,
                TotalLines,
                TotalVotes,
            ): (ScriptId, ScriptContent)
        )

        # Make a map of files to functions
        file_to_functions = loaded_data.flatMap(lambda row: run_lexer(row))
        # Map lines to functions, to join with the author data we need the script_id to be an integer
        self.lines_to_functions = file_to_functions.map(
                lambda (script_id, (line_num, cont_type, cont)):
                (int(float(script_id)), clean_names(cont))
            )

    def _set_scripts_to_authors(self):
        self.author_data = self.user_interactions.map(
            lambda (
                AuthorUserId,
                CurrentScriptVersionId,
                FirstScriptVersionId,
                ForkParentScriptVersionId,
                ForumTopicId,
                Id_,
                IsProjectLanguageTemplate,
                ScriptProjectId,
                TotalViews,
                TotalVotes,
                UrlSlug
            ): (Id_, AuthorUserId)
        )

    def __join_authors_to_functions(self):

        # We must deepcopy the map so that Spark can pack up the dictionary and
        # ship it around our cluster. If we do not deepcopy, Spark tries to
        # pack up this whole class which fails because their are RDDs stored in
        # the class.
        function_map = self.sc.broadcast(deepcopy(self.function_map))

        script_functionid = self.lines_to_functions.map(
                lambda (script_id, function): (script_id, function_map.value[function])
            )

        self.authorid_functionid = script_functionid.join(self.author_data).map(
                lambda (script_id, (function_id, author_id)): (author_id, function_id)
        )

    def __set_function_map(self):
        functions = self.lines_to_functions\
            .map(lambda (line, function): function)\
            .distinct()

        self.function_map = {k: v for v, k in enumerate(functions.collect())}

    def get_user_vector(self):

        """Produce an RDD containing tuples of the form (user, item, rating).

        There is currently three options when producing these user vectors:
            forked_interact: The interactions a user has with a forked script

        """

        if self.user_vector_type == 'forked_interact':
            # The option 'forked_interact' currently has a different object id, so would have issues in a content based setting
            # We are going to recommend based on the forked parent, we really only want those parents though with 10 or more children forks

            self.user_interactions.registerTempTable('scripts')

            forked_scripts = self.sqlCtx.sql("select ForkParentScriptVersionId, count(*) from scripts \
                where ForkParentScriptVersionId!='' group by ForkParentScriptVersionId")\
                .map(lambda row: (int(float(row.ForkParentScriptVersionId)), row._c1)).filter(lambda (i,c):c>10)\
                .map(lambda (v_id, c1): ("%.1f"%v_id, c1))
            fields = [StructField("script_id", StringType(), True), StructField("v_count", IntegerType(), True)]
            schema = StructType(fields)

            good_script_ids = self.sqlCtx.createDataFrame(forked_scripts, schema)
            good_script_ids.registerTempTable("recommendable_ids")

            recommendable = self.sqlCtx.sql("select scripts.* from scripts, recommendable_ids  where ForkParentScriptVersionId = script_id")
            recommendable.registerTempTable("recommendable")

            user_info = self.sqlCtx.sql("select AuthorUserId, ForkParentScriptVersionId, 1 from recommendable group by AuthorUserId, ForkParentScriptVersionId").map(lambda (u,i,r): (int(float(u)), int(float(i)), r))

            return user_info

        # Others start as (author_id, function_id) with duplicates
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
            print "Please choose a user_vector_type from 'forked_interact', 'any_interact', 'num_interact', or None"
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
            model = self.sc.broadcast(deepcopy(word_model))
            function_map = self.sc.broadcast(deepcopy(self.function_map))

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
    author_id, content = row


    # The content will come in one of two ways: a string of the full code, or a dictionary of the notebook cells
    # We will simply try to change it into a json

    try:
        json_content = json.loads(content)
        all_code = ""
        for cell in json_content['cells']:
            if cell['cell_type']=='code':
                all_code = all_code + cell['source'] + "\n"
        content = all_code
    except:
        pass

    new_content = []
    # There is a bug in Python when the first line is a coding statement:
    # https://bugs.python.org/issue22221
    for line in content.splitlines():
        if not (line.startswith("#") or "coding:" in line or '%' in line):
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
                output.append((author_id, item))

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

