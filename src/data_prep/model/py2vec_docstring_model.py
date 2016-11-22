import ast
import inspect
import imp
import numpy as np
import os
import re
from pyspark.mllib.feature import Word2Vec

# =============================================================================
# Helper Functions
# 
# These helper functions will be used in Py2VecModel's __get_file_docstrings()
# 
# These helper functions extract the docstring of each:
# * function definition
# * class definition
# * class's function definition
# * import library
# * from ... import ... library
# 
# =============================================================================

def getModule(parent_module_name, this_module_name):
    # this implementation only works on python 2.7
    parent_module = __import__(parent_module_name, globals(), locals(), this_module_name)
    if this_module_name is None:
        return parent_module
    else:
        this_module = getattr(parent_module_name, this_module_name)
        return this_module
    
"""
import importlib
def getModule(parent_module_name, this_module_name):
    # this implementation only works on python 3
    parent_module_name = importlib.import_module(parent_module_name)
    if this_module_name is None:
        return parent_module
    else: 
        this_module = getattr(parent_module_name, this_module_name)
        return this_module
"""

def __get_repo_docstring(repo_name):
    try:
        repo_module = getModule(repo_name, None)
        docstring = inspect.getdoc(repo_module)
        return docstring
    except Exception:
        return ""


def __get_file_docstring(file_path):
    # this function does not grab the docstring of intermediary modules
    # ie. grandparent_module.parent_module.child_module.granchild_module
    # this function will only grab the docstring of child_module and grandchild_module
    # but not grandparent_module or parent_module
    try:
        parent_module_name = os.path.dirname(file_path).replace("/", ".")
        this_module_name = os.path.splitext(os.path.basename(file_path))[0]
        docstring = inspect.getdoc(getModule(str(parent_module_name), None))
        docstring += inspect.getdoc(getModule(str(parent_module_name), str(this_module_name)))
        return docstring
    except Exception:
        return ""

def __get_import_docstring(ast_module):
    # this function does not grab the docstring of 
    # import libraries within the same project
    
    try:
        # get import library docstring
        import_definitions =  [node for node in ast_module.body if isinstance(node, ast.Import)]
        docstring = ""
        for import_definition in import_definitions:
            import_alias = import_definition.names[0]
            import_module_name = import_alias.name
            import_module = getModule(import_module_name, None)
            docstring += inspect.getdoc(import_module)
        return docstring
    except Exception:
        return ""

def __get_import_from_docstring(ast_module):
    # this function does not grab the docstring of 
    # import libraries within the same project
    
    try:
        # get import library docstring
        import_definitions =  [node for node in ast_module.body if isinstance(node, ast.ImportFrom)]
        docstring = ""
        for import_definition in import_definitions:
            import_alias = import_definition.names[0]
            import_module_name = import_alias.name
            import_module = getModule(import_module_name, None)
            tmp_docstring = inspect.getdoc(import_module)
            if tmp_docstring is not None:
                docstring += tmp_docstring
        return docstring
    except Exception:
        return ""

def __get_function_docstring(ast_module):
    # this function grabs all functions' docstrings in a file
    try:
        function_definitions = [node for node in ast_module.body if isinstance(node, ast.FunctionDef)]
        docstring = ""
        for function_definition in function_definitions:
            #function_name = function_definition.name
            function_docstring = ast.get_docstring(function_definition)
            if function_docstring is not None:
                docstring += function_docstring
        return docstring
    except Exception:
        return ""

def __get_class_docstring(ast_module):
    # this function grab all classes' docstrings in a file
    # as well as each class's functions' docstrings
    try:
        class_definitions = [node for node in ast_module.body if isinstance(node, ast.ClassDef)]
        docstring = ""
        for class_definition in class_definitions:
            #class_name = class_definition.name
            class_docstring = ast.get_docstring(class_definition)
            if class_docstring is not None:
                docstring += class_docstring
            # add the class's functions' docstrings too!
            docstring += __get_class_function_docstring(class_definition.body)
        return docstring
    except Exception:
        return ""
        
def __get_class_function_docstring(function_definitions):
    # this function grabs the class's functions' docsstrings
    # TODO: integrate this with __get_function_docstring
    try:
        docstring = ""
        for function_definition in function_definitions:
            if isinstance(function_definition, ast.FunctionDef):
                #function_name = function_definition.name
                function_docstring = ast.get_docstring(function_definition)
                if function_docstring is not None:
                    docstring += function_docstring
        return docstring
    except Exception:
        return ""

def get_docstring(((repo_name, file_path), file_lines)):
    # returns [((repo_name, file_path), file_docstrings)]
    docstring = ""
    docstring = __get_repo_docstring(repo_name)
    docstring += __get_file_docstring(file_path)
    try:
        # get ast's module from file's lines
        ast_module = ast.parse(file_lines)
    except Exception:
        pass
    else:
        docstring += __get_import_docstring(ast_module)
        docstring += __get_import_from_docstring(ast_module)
        docstring += __get_function_docstring(ast_module)
        docstring += __get_class_docstring(ast_module)
        
    return ((repo_name, file_path), docstring)

# =============================================================================
# Py2VecModel 
# takes in the dataframe of the JSON file and the setting of Word2Vec
# =============================================================================

class Py2VecModel(object):
    def __init__(self, gitdf, word2vec_setting=[20, 41, 0.025, 50]):
        # gitdf is a dataframe of your data's JSON file 
        self.gitdf = gitdf 
        self.word2vec = self.__get_word2vec(word2vec_setting)

    def __get_word2vec(self, word2vec_setting):
        min_count, seed, learning_rate, vector_size = word2vec_setting
        word2vec = Word2Vec()
        # Word2Vec's default min count is 100; our default min count is 20.
        word2vec.setMinCount(min_count)
        word2vec.setSeed(seed)
        # Word2Vec's default learning rate is 0.025; our default min count is also 0.025.
        word2vec.setLearningRate(learning_rate)
        # Word2Vec's default vector size is 100; our default vector size is 50.
        word2vec.setVectorSize(vector_size)
        return word2vec

    def get_model(self):
        raise NotImplemented

    def get_model_dict(self):
        raise NotImplemented

# =============================================================================
# Py2VecDocstringModel 
# You want to pass in get_model_dict() from this object to git_vectorize() 
# as a support file.
# 
# How to use it:
# 1. py2vecDocstringModel = Py2VecDocstringModel(gitdf) 
#    or 
#    py2vecDocstringModel = Py2VecDocstringModel(gitdf, [20, 41, 0.025, 50])
# 2. model_dict = py2vecDocstringModel.get_model_dict()
# 3. vectorizer = git_vectorize(gitdf, None, "py2vec", sc, model=model_dict)
# 4. content_vector = vectorizer.get_content_vector()
# 
# =============================================================================

class Py2VecDocstringModel(Py2VecModel):
    def get_model(self):
        wordstrings = self.__get_each_word_in_docstrings()
        return self.word2vec.fit(wordstrings)

    def get_model_dict(self):
        model = self.get_model()
        return {k:np.array(list(v)) for k,v in dict(model.getVectors()).iteritems()}

    def __get_code_lines(self):
        """
        extract all lines in files
        output: code_lines == [((reponame, filename), (line_num, line))]
        """
        code_lines = self.gitdf.map(
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
                reponame,
            ):
            ((reponame, filename), (line_num, line))
        ).cache()
        return code_lines
    
    def __get_file_lines(self):
        """
        append each file's code lines
        output: file_lines == [((reponame, filename), filelines)]
        """
        # code_lines == [((reponame, filename), (line_num, line))]
        code_lines = self.__get_code_lines()
        # file_lines == [((reponame, filename), [(line_num_1, line_1), ..., (line_num_n, line_n)])]
        file_lines = code_lines.mapValues(lambda val:[val]).reduceByKey(lambda a, b: a + b)
        # sort each file's code lines by line number and extract only the lines from a file
        def sortByLineNumberAndExtractLines(listOfCodeLines):
            sortedListOfCodeLines = sorted(listOfCodeLines, key=lambda (line_num, line): line_num)
            fileLines = ""
            for lineNum, line in sortedListOfCodeLines:
                fileLines += line + "\n"
            return fileLines
        # file_lines == [((reponame, filename), filelines)]
        file_lines = file_lines.mapValues(lambda listOfCodeLines: sortByLineNumberAndExtractLines(listOfCodeLines))
        return file_lines

    def __get_file_docstrings(self):
        """
        get docstring of each file
        output: file_docstrings == [((reponame, filepath), docstring)]
        """
        # file_lines == [((reponame, filename), filelines)]
        file_lines = self.__get_file_lines()
        file_docstrings = file_lines.map(
            lambda ((repo_name, file_path), file_lines): get_docstring(((repo_name, file_path), file_lines))
        )
        return file_docstrings

    def __get_docstrings(self):
        """
        get specifically just the docstring of all files
        output: docstrings == [docstring]
        """
        file_docstrings = self.__get_file_docstrings()
        docstrings = file_docstrings.map(lambda ((repo_name, file_path), docstring): docstring)
        return docstrings

    def __get_each_word_in_docstrings(self):
        """
        get each word from the docstring of all files
        output: wordstrings == [[word1, word2, ..., wordn]]
        """
        docstrings = self.__get_docstrings()
        wordstrings = docstrings.map(lambda docstring: re.sub("[^\w]", " ", docstring).split())
        return wordstrings
