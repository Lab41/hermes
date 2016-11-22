from src.utils import glove
import numpy as np
import string


class jester_vectorize():

    def __init__(self, user_interactions, content, user_vector_type, content_vector_type, **support_files):
        """Set up the Jester Vectorizer.

        Args:
            user_interactions (rdd): The raw data of users interactions with
                the system. For Jester, each "row" is as follows:
                Row(joke_id, rating, user_id)
            content (rdd): The raw data about the items in the dataset. For
                Jester, each row is as follows: Row(joke_id, joke_text)
            user_vector_type (str): The type of user vector desired. One of
                'ratings', 'pos_ratings', 'ratings_to_interact', or None.
            content_vector_type: The type of content vector desired. One of
                'glove' or None.
            support_files: Only one support file is used for this class:
                glove_model: An instantiated glove model.

        """
        self.user_vector_type = user_vector_type
        self.content_vector_type = content_vector_type

        self.user_interactions = user_interactions
        self.content = content

        # If no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}

    def get_user_vector(self):
        """Produce an RDD containing tuples of the form (user, item, rating).

        There are three options when producing these user vectors:

            ratings: The ratings the users assigned
            pos_ratings: Only ratings > 0, all others are discarded
            ratings_to_interact: Positive ratings are mapped to 1, negative to -1.

        """
        uir = self.user_interactions.map(lambda row: (row.user_id, row.joke_id, row.rating))

        if self.user_vector_type == 'ratings':
            return uir

        elif self.user_vector_type == 'pos_ratings':
            return uir.filter(lambda u_i_r: u_i_r[2] > 0)

        elif self.user_vector_type == 'ratings_to_interact':
            return uir.map(lambda u_i_r1: (u_i_r1[0], u_i_r1[1], 1 if u_i_r1[2] > 0 else -1))

        elif self.user_vector_type == 'none' or self.user_vector_type is None:
            return None

        else:
            print("Please choose a user_vector_type between 'ratings', 'pos_ratings', 'ratings_to_interact', and 'none'")
            return None

    def get_content_vector(self):
        """Produce an RDD containing tuples of the form (item, content_vector).

        There is one method of producing content vectors:

            glove: Use the Stanford GloVe model to sum vector ratings of all
                the words in the joke.

        """
        if self.content_vector_type == 'glove':

            # The model is initialized by the user and passed in via the
            # support_file object
            glove_model = self.support_files["glove_model"]

            # Transformation function
            def joke_to_glove(row, glove):
                vector = np.zeros(glove.vector_size)
                for chunk in row.joke_text.split():
                    word = chunk.lower().strip(string.punctuation)
                    vector += glove[word]

                return (row.joke_id, vector)

            # Run the transformation function over the data
            return self.content.map(lambda row: joke_to_glove(row, glove_model))

        elif self.content_vector_type == 'none' or self.content_vector_type is None:
            return None

        else:
            print("Please choose a content_vector_type between 'glove' or None")
            return None
