import ast
import inspect
import imp
import os

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
    parent_module_name = os.path.dirname(file_path).replace("/", ".")
    this_module_name = os.path.splitext(os.path.basename(file_path))[0]
    try:
        docstring = inspect.getdoc(getModule(str(parent_module_name), None))
        docstring += inspect.getdoc(getModule(str(parent_module_name), str(this_module_name)))
        return docstring
    except Exception:
        return ""

def __get_import_docstring(file_lines):
    # this function does not grab the docstring of 
    # import libraries within the same project
    try:
        # get ast's module from file's lines
        ast_module = ast.parse(file_lines)
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

def __get_function_docstring(file_lines):
    return ""

def ___get_class_docstring(file_lines):
    return ""

def __get_class_function_docstring(file_lines):
    return ""

def get_docstring(((repo_name, file_path), file_lines)):
    # returns [((repo_name, file_path), file_docstrings)]
    docstring = __get_repo_docstring(repo_name)
    docstring += __get_file_docstring(file_path)
    docstring += __get_import_docstring(file_lines)
    docstring += __get_function_docstring(file_lines)
    docstring += __get_class_docstring(file_lines)
    docstring += __get_class_function_docstring(file_lines)
    return ((repo_name, file_path), docstring)