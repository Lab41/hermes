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
        docstring += __get_function_docstring(ast_module)
        docstring += __get_class_docstring(ast_module)
        
    return ((repo_name, file_path), docstring)