# This script is an example for how to run the Hermes code base for Kaggle
# To run simply change the filepaths to point to the correct input and output locations for your datasets

# There will be slight differences for other datasets, primarily the data_name, user and content_vector_types variables


sc.addPyFile('/home/abethke/hermes.zip')
from src import hermes_run_script

interactions = sqlCtx.read.json('datasets/scripts.json').sample(False, 0.5,41)

code_content = sqlCtx.read.json('datasets/script_versions.json').sample(False, 0.5,41)

from src.utils import Py2Vec


# This file is a support file and may be found at https://github.com/Lab41/Misc/tree/master/blog/python2vec/data
# It is a trained Python2Vec model
json_file = "datasets/blog_model.json"
model = Py2Vec.Py2Vec(json_file)


#name of the dataset: will be used for to get the correct vectorizer and when saving files
data_name = 'kaggle'
#the types of user vectors to assess
#each dataset has different user vectors that can be chosen
user_vector_types = ['any_interact', 'num_interact']
#the types of content vectors to assess
#each dataset has different content vectors that can be chosen
content_vector_types = ['py2vec']

#the directory where intermediate files will be saved including user vectors, content vectors, and predictions
#this can be HDFS
directory = 'kaggle_results/data'

#the directory for the csv results files.
#this should not be HDFS
#For many types of systems you must create the file path first
results_directory = 'kaggle_results/results/'

#the collaborative filtering algorithms to run
cf_predictions = ['cf_mllib','cf_item', 'cf_user' ]
#the content based algorithms to run
cb_predictions = ['cb_vect', 'cb_kmeans_100', 'cb_kmeans_1000']

#the number of predictions to give to a user
result_runs = [100,1000]

#any additional items that are necessary to run the content vectors
#for Kaggle this includes a trained Python2Vec model
support_files = {'model':model.get_model()}

runner = hermes_run_script.hermes_run(interactions, code_content, user_vector_types, content_vector_types,\
    sqlCtx, sc, data_name, directory, results_directory, cf_predictions, cb_predictions, \
    result_runs, num_partitions=30, **support_files)

#run the vectorizer to get the user and content vectors necessary for recommendations
runner.run_vectorizer()

#run the collaborative filtering algorithms
runner.run_cf_predictions()

#run the content based algorithms
runner.run_cb_predictions()

#get the results for the collaborative filtering predictions
runner.run_cf_results()

#get the results for the content based predictions
runner.run_cb_results()

#the last step combines all previous results into one csv file
runner.consolidate_results()