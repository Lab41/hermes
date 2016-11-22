import json
import numpy as np

class Py2Vec(object):
    """Load a Py2Vec JSON model and provide access to the vectors and vector
    space.
    Provides access to word vectors as if it were a dictionary:
        py2vec = Py2Vec('file')
        py2vec['word']
    Unrecognized words will return the Null vector (all 0).
    Also provides a way to find the closest word to a vector in the vector
    space.
    Args:
        json_file (str): Location of the JSON Py2Vec file.
    Attributes:
        null_vector (numpy array): The null (0) vector as a numpy array. It has
            the correct size for the model's vector space.
        vector_size (int): The number of dimensions in the vector space.
    """
    def __init__(self, json_file):
        self.__model = {}
        self.__line_to_word = {}
        space = []
        # Load the Py2Vec data from a file
        with open(json_file, 'r') as open_file:
            tmp_model = json.load(open_file)
            self.__model = {k: np.array(v) for k, v in tmp_model.items()}

            for line_number, key_word in enumerate(self.__model):
                vector = self.__model[key_word]
                self.__line_to_word[line_number] = key_word
                space.append(vector)

        # Set up a vector space so we can quickly find the closest vector
        self.__vector_space = np.array(space)

        # Null vector for unrecognized words
        self.vector_size = len(vector)
        self.null_vector = np.zeros(self.vector_size)

    def get_model(self):
        """Return the dictionary model that is used for the code content vectorizer functions (kaggle, git)
        Args:
            None
        Returns:
            __model: The trained py2vec model as a dictionary
        """
        return self.__model

    def __getitem__(self, key):
        """Return the vector representation of a word.
        Args:
            key (str): A word to locate in the vector space.
        Returns:
            numpy array: The location of the word in the vector space, or the
                null (0) vector if the word is not found.
        """
        return self.__model.get(key, self.null_vector)

    def closest_words(self, input_arg, n=1):
        """Return the n closest word to a given vector.
        Args:
            input_arg (str or numpy array): Either a string of a word in the
                model, or a vector of the same dimension as the vector space.
            n (Optional[int]): The number of values to return. Defaults to 1.
        Returns:
            list of tuples: A list containing tuples of the form:
                (distance, word). None is returned if a string was provided as
                an argument that is not in the model.
        """
        # If you gave us a word, find the vector, otherwise if the word is not
        # in the model return None.
        if isinstance(input_arg, str):
            key = input_arg.lower()
            vector = self.__model.get(key, None)
            if vector is None:
                return None
        else:
            vector = input_arg

        # Find the closest vectors, note that we use n+1 because we sometimes
        # discard the vector with distance == 0 and we still want to have n
        # results.
        squares = (self.__vector_space - vector)**2
        distances = np.sum(squares, axis=1)
        line_numbers = np.argpartition(distances, n+1)[:n+1]

        # argpartition partitions the list around the nth element, but does not
        # guarantee the order is correct, so we have to sort.
        output = []
        for line_number in line_numbers:
            dist = distances[line_number]
            # Throw out identical vectors, there should be only one
            if dist == 0:
                continue

            word = self.__line_to_word[line_number]
            output.append((round(dist, 3), word))

        return sorted(output)[:n]
