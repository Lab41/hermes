import numpy as np

class Glove(object):
    """Load a GloVe model and provide access to the vectors and vector space.

    Provides access to word vectors as if it were a dictionary:

        glove_instance = Glove('file')
        glove['word']

    Unrecognized words will return the Null vector (all 0).

    Also provides a way to find the closest word to a vector in the vector
    space.

    """

    def __init__(self, glove_file):
        """Set up the GloVe class by reading in a vector file.

        Args:
            glove_file (str): Location of the plain text GloVe vector file.

        """
        self.__model = {}
        self.__line_to_word = {}
        space = []
        # Load the GloVe data from a file
        with open(glove_file, 'r') as open_file:
            for line_number, line in enumerate(open_file):
                sline = line.split()
                key_word = sline[0]
                vector = np.array([float(i) for i in sline[1:]])
                self.__model[key_word] = vector
                self.__line_to_word[line_number] = key_word
                space.append(vector)

        # Set up a vector space so we can quickly find the closest vector
        self.__vector_space = np.array(space)

        # Null vector for unrecognized words
        self.vector_size = len(vector)
        self.__null_vector = np.zeros(self.vector_size)

    def __getitem__(self, key):
        """Return the vector representation of a word.

        Args:
            key (str): A word to locate in the vector space.

        Returns:
            numpy array: The location of the word in the vector space, or the
                null (0) vector if the word is not found.

        """
        return self.__model.get(key, self.__null_vector)

    def closest_word(self, vector):
        """Return the closest word to a given vector.

        Args:
            vector (numpy array): A vector of the same dimension as the vector
                space.

        Returns:
            str: The closest word to the input vector in the vector space.
        """
        squares = (self.__vector_space - vector)**2
        distances = np.sum(squares, axis=1)
        line_number = np.argmin(distances)
        return self.__line_to_word[line_number]
