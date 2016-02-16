import os.path
from src.utils import save_load as sl
from src.algorithms import content_based_kmeans, content_based, cf, performance_metrics

class hermes_run():

    def __init__(self, user_interactions, content, user_vector_types, content_vector_types, sqlCtx, sc, data_name, directory, results_directory, cf_predictions, cb_predictions, results_runs,  **support_files ):
        """
        Class initializer to load the required files

        Args:
            user_interactions: The raw RDD of the user interactions. For MovieLens, these are the ratings
            content: The raw RDD containing the item content. For MovieLens, this is the movie categories
            user_vector_types: The types of user vector you would like to run.  For MovieLens this could be ['ratings', 'pos_ratings', 'ratings_to_interact'.
            content_vector_type: The type of content vector you would like to run. For MovieLens this could be ['genre', 'tags'].
            sqlCtx: The SQLCtx which is needed for the predictions and some user or content vectors
            sc: The spark content which is needed to save files
            data_name: The name of the data to be used when saving files.  like 'movielens_1m' or 'jester'
            directory: The directory to save the user, content and prediction vectors.  Most likely HDFS
            results_directory: A non-HDFS directory to save the csv results.
            cf_predictions: The CF algorithms to predict with.  These could be ['cf_mllib', 'cf_item', 'cf_user']
            cb_predictions: The CB algorithms to predict with.  These could be ['cb_vect', 'cb_kmeans_100', 'cb_kmeans_1000']
            results_runs: The N to pass into predictions.  These are [100, 1000].
                1000 is used as it is recommended for corpus analysis, and 100 because Jester only has 150 items
            support_files: If they exist, the supporting files, dataFrames, and/or file links necessary to run the content vectors.
                To generate a content vector on tags, you must pass in the tag RDD as support_files['tag_rdd']
                You may also pass in the number of tags to utilize as support_files['num_tags'].  Otherwise default is set to 300


        """
        self.user_vector_types = user_vector_types
        self.content_vector_types = content_vector_types

        #Filter out uninteresting articles and users if they still exist in the dataset
        self.user_interactions =user_interactions
        self.user_interactions.registerTempTable("ratings")
        self.content = content
        self.content.registerTempTable("content")

        self.sqlCtx = sqlCtx

        self.data_name = data_name

        self.directory = directory
        self.results_directory = results_directory

        self.cf_predictions = cf_predictions
        self.cb_predictions = cb_predictions

        self.results_runs = results_runs

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def run_cf_predictions(self):
        for uv in self.user_vector_types:
            train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
            train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc)

            for cf_pred in self.cf_predictions:

                pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cf_pred  + '.pkl'

                if os.path.isdir(pred_save_loc)==False:
                    print 'Running ' + cf_pred + ' for user vector ' + uv
                    print pred_save_loc
                    if cf_pred=='cf_mllib':
                        predictions = cf.calc_cf_mllib(train_ratings, num_partitions=60)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    elif cf_pred=='cf_item':
                        predictions = cf.calc_item_item_cf(train_ratings, num_partitions=60)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    elif cf_pred=='cf_user':
                        predictions = cf.calc_user_user_cf2(train_ratings, num_partitions=60)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    else:
                        break
        print 'All CF predictions saved'

    def run_cb_predictions(self):
        for cv in self.content_vector_types:
            content_path = self.directory + self.data_name +'_cv_' + cv + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc)
            print content_vect.count()

            for uv in self.user_vector_types:
                train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
                train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc)

                for cb_pred in self.cb_predictions:
                    pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cv + '_' + cb_pred  + '.pkl'
                    print pred_save_loc
                    if os.path.isdir(pred_save_loc)==False:
                        print 'Running ' + cb_pred + ' for user vector ' + uv + ' and content vector ' + cv
                        if cb_pred=='cb_vect':
                            predictions = content_based.predict(train_ratings, content_vect, num_partitions=60)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        elif cb_pred=='cb_kmeans_100':
                            predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=100)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        elif cb_pred=='cb_kmeans_1000':
                            predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=1000)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        else:
                            break
        print 'All CB predictions saved'


    def run_cf_results(self):
        for uv in self.user_vector_types:
            train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
            train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc)
            test_ratings_loc = self.directory + self.data_name + '_uv_test_' + uv + '.pkl'
            test_ratings = sl.load_from_hadoop(test_ratings_loc, self.sc)

            #get the first content vector for results purposes
            content_path = self.directory + self.data_name +'_cv_' + self.content_vector_types[0] + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc)

            for cf_pred in self.cf_predictions:

                pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cf_pred  + '.pkl'
                print 'Getting results for: ' + pred_save_loc
                preds = sl.load_from_hadoop(pred_save_loc, self.sc)

                for run in self.result_runs:
                    results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                    content_vect, self.sqlCtx, num_predictions = run)
                    #add some information to the results dictionary if it gets jumbled

                    results['N'] = run
                    results['dataset'] = self.data_name
                    results['CF_CB'] = 'CF'
                    results['alg_type'] = cf_pred
                    results['user_vector'] = uv
                    results['content_vector'] = self.content_vector_types[0]
                    print results

                    #save off the results
                    results_path = self.results_directory + self.data_name + '_results_' + uv + '_' \
                                + cf_pred  + str(run) + '.pkl'
                    f = open(results_path, 'w')
                    f.write(str(results))
                    f.close()
        print 'All CF predictions results aquired'


    def run_cb_results(self):
        for cv in self.content_vector_types:
            content_path = self.directory + self.data_name +'_cv_' + cv + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc)
            for uv in self.user_vector_types:
                train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
                train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc)
                test_ratings_loc = self.directory + self.data_name + '_uv_test_' + uv + '.pkl'
                test_ratings = sl.load_from_hadoop(test_ratings_loc, self.sc)

                content_path = self.directory + self.data_name +'_cv_' + cv + '.pkl'
                content_vect = sl.load_from_hadoop(content_path, self.sc)

                for cb_pred in self.cb_predictions:

                    pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cv + '_' \
                                + cb_pred + '.pkl'
                    print 'Getting results for: ' + pred_save_loc
                    preds = sl.load_from_hadoop(pred_save_loc, self.sc)
                    #print preds.count()

                    #if we ran the kmeans we do not need to complete both runs
                    #otherwise we do
                    if cb_pred=='cb_kmeans_100' or cb_pred=='cb_kmeans_1000':
                        if cb_pred=='cb_kmeans_1000':
                            run = 1000
                        else:
                            run = 100
                        results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                            content_vect, self.sqlCtx, num_predictions = run)
                        #add some information to the results dictionary if it gets jumbled
                        results['N'] = run
                        results['dataset'] = self.data_name
                        results['CF_CB'] = 'CB'
                        results['alg_type'] = cb_pred
                        results['user_vector'] = uv
                        results['content_vector'] = cv
                        print results

                        #save off the results
                        results_path = self.results_directory + self.data_name + '_results_' + uv + '_' + cv + '_' \
                                        + cb_pred  + '_' + str(run) + '.csv'
                        print results_path
                        f = open(results_path, 'w')
                        f.write(str(results))
                        f.close()


                    else:
                        for run in self.result_runs:
                            results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                            content_vect, self.sqlCtx, num_predictions = run)
                            #add some information to the results dictionary if it gets jumbled
                            results['N'] = run
                            results['dataset'] = self.data_name
                            results['CF_CB'] = 'CB'
                            results['alg_type'] = cb_pred
                            results['user_vector'] = uv
                            results['content_vector'] = cv
                            print results

                            #save off the results
                            results_path = self.results_directory + self.data_name + '_results_' + uv + '_' + cv \
                                        + '_' + cb_pred  + '_' + str(run) + '.csv'
                            print results_path
                            f = open(results_path, 'w')
                            f.write(str(results))
                            f.close()
        print 'All CB predictions results aquired'