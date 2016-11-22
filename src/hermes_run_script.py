import os.path
from src.utils import save_load as sl
from src.algorithms import content_based_kmeans, content_based, cf, performance_metrics, dataset_stats, random_recommender
from src.data_prep import book_vectorize, git_vectorize, jester_vectorize, last_fm_vectorize, movieLens_vectorize, osm_vectorize, wiki_vectorize, kaggle_vectorize
import csv
import pandas as pd

class hermes_run():

    def __init__(self, user_interactions, content, user_vector_types, content_vector_types, sqlCtx, sc, data_name, directory, results_directory, cf_predictions, cb_predictions, results_runs, num_partitions=60, **support_files ):
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
        self.content = content

        self.sqlCtx = sqlCtx
        self.sc = sc

        self.data_name = data_name

        self.directory = directory + '/' #ensure directory is given
        self.results_directory = results_directory  + '/'  #ensure directory is given

        self.cf_predictions = cf_predictions
        self.cb_predictions = cb_predictions

        self.results_runs = results_runs

        self.num_partitions = num_partitions

        #if no support files were passed in, initialize an empty support file
        if support_files:
            self.support_files = support_files
        else:
            self.support_files = {}


    def run_vectorizer(self):
        for uv in self.user_vector_types:
            #set up the vectorizer.  The data going into each will be slightly different
            #Some don't have sqlCtx and some need support vectors
            vectorizor = self.get_vectorizer(uv, self.content_vector_types[0])

            user_info = vectorizor.get_user_vector().repartition(20)
            train_ratings, test_ratings = user_info.randomSplit([0.9,0.1], 41)

            uv_train_path = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
            uv_test_path = self.directory + self.data_name+ '_uv_test_' + uv + '.pkl'

            #ensures we don't write over the original
            if os.path.isdir(uv_train_path)==False:
                print uv_train_path
                sl.save_to_hadoop(train_ratings, uv_train_path)
            if os.path.isdir(uv_test_path)==False:
                print uv_test_path
                sl.save_to_hadoop(test_ratings, uv_test_path)

        for cv in self.content_vector_types:
            vectorizor = self.get_vectorizer(uv, cv)
            content_vector = vectorizor.get_content_vector()

            content_path = self.directory + self.data_name +'_cv_' + cv + '.pkl'

            if os.path.isdir(content_path)==False:
                print content_path
                sl.save_to_hadoop(content_vector, content_path)

    def get_vectorizer(self, user_vector_type, content_vector_type, ):

        #get the vectorizer based on the name of the data_name
        #this does require the user to put in the name correctly, but it is the simplest to use

        if self.data_name == 'book_crossing':
            return book_vectorize.book_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, self.sc, **self.support_files )
        elif self.data_name == 'git':
            return git_vectorize.git_vectorize(self.user_interactions, user_vector_type, content_vector_type, self.sc, **self.support_files )
        elif self.data_name == 'jester':
            return jester_vectorize.jester_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, **self.support_files )
        elif self.data_name == 'last_fm':
            return last_fm_vectorize.last_fm_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, **self.support_files )
        elif self.data_name.startswith('movielens'):
            return movieLens_vectorize.movieLens_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, **self.support_files )
        elif self.data_name.startswith('osm'):
            return osm_vectorize.osm_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, **self.support_files )
        elif self.data_name.startswith('wiki'):
            return wiki_vectorize.wiki_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, **self.support_files )
        elif self.data_name == 'kaggle':
            return kaggle_vectorize.kaggle_vectorize(self.user_interactions, self.content, user_vector_type, content_vector_type, self.sqlCtx, self.sc, **self.support_files )

        else:
            print 'Please pass in either, book_crossing, git, jester, last_fm, movielens_1m, movielens_10m, movielens_20m, osm, or wiki'

    def run_single_prediction(self, user_vector, content_vector, alg_type):
        train_ratings_loc = self.directory + self.data_name + '_uv_train_' + user_vector + '.pkl'
        train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)

        if content_vector:
            content_path = self.directory + self.data_name +'_cv_' + content_vector + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc).repartition(self.num_partitions)
            print 'Running ' + alg_type + ' for user vector ' + user_vector + ' and content vector ' + content_vector

            pred_save_loc = self.directory + self.data_name + '_predictions_' + user_vector + '_' + content_vector + '_' + alg_type  + '.pkl'
            print pred_save_loc

            if alg_type=='cb_vect':
                predictions = content_based.predict(train_ratings, content_vect, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cb_kmeans_100':
                predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=100, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cb_kmeans_1000':
                predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=1000, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)

        else:
            print 'Running ' + alg_type + ' for user vector ' + user_vector

            pred_save_loc = self.directory + self.data_name + '_predictions_' + user_vector + '_' + alg_type  + '.pkl'
            print pred_save_loc

            if alg_type=='cf_mllib':
                predictions = cf.calc_cf_mllib(train_ratings, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_item':
                predictions = cf.calc_item_item_cf(train_ratings, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_user':
                predictions = cf.calc_user_user_cf2(train_ratings, num_partitions=self.num_partitions)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_bayes_map':
                predictions = cf.calc_naive_bayes_map(train_ratings, self.sc)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_bayes_mse':
                predictions = cf.calc_naive_bayes_mse(train_ratings, self.sc)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_bayes_mae':
                predictions = cf.calc_naive_bayes_mae(train_ratings, self.sc)
                sl.save_to_hadoop(predictions, pred_save_loc)
            elif alg_type=='cf_random':
                predictions = random_recommender.predict(train_ratings, self.sc)
                sl.save_to_hadoop(predictions, pred_save_loc)


    def run_cf_predictions(self):
        for uv in self.user_vector_types:
            train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
            train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)

            for cf_pred in self.cf_predictions:

                pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cf_pred  + '.pkl'

                if os.path.isdir(pred_save_loc)==False:
                    print 'Running ' + cf_pred + ' for user vector ' + uv
                    print pred_save_loc
                    if cf_pred=='cf_mllib':
                        predictions = cf.calc_cf_mllib(train_ratings, num_partitions=self.num_partitions)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    elif cf_pred=='cf_item':
                        predictions = cf.calc_item_item_cf(train_ratings, num_partitions=self.num_partitions)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    elif cf_pred=='cf_user':
                        predictions = cf.calc_user_user_cf2(train_ratings, num_partitions=self.num_partitions)
                        sl.save_to_hadoop(predictions, pred_save_loc)
                    else:
                        break
        print 'All CF predictions saved'

    def run_cb_predictions(self):
        for cv in self.content_vector_types:
            content_path = self.directory + self.data_name +'_cv_' + cv + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc).repartition(self.num_partitions)

            for uv in self.user_vector_types:
                train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
                train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)

                for cb_pred in self.cb_predictions:
                    pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cv + '_' + cb_pred  + '.pkl'
                    print pred_save_loc
                    if os.path.isdir(pred_save_loc)==False:
                        print 'Running ' + cb_pred + ' for user vector ' + uv + ' and content vector ' + cv
                        if cb_pred=='cb_vect':
                            predictions = content_based.predict(train_ratings, content_vect, num_partitions=self.num_partitions)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        elif cb_pred=='cb_kmeans_100':
                            predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=100, num_partitions=self.num_partitions)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        elif cb_pred=='cb_kmeans_1000':
                            predictions = content_based_kmeans.predict(train_ratings, content_vect, num_predictions=1000, num_partitions=self.num_partitions)
                            sl.save_to_hadoop(predictions, pred_save_loc)
                        else:
                            break
        print 'All CB predictions saved'


    def run_cf_results(self):
        for uv in self.user_vector_types:
            train_ratings_loc = self.directory + self.data_name + '_uv_train_' + uv + '.pkl'
            train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)
            test_ratings_loc = self.directory + self.data_name + '_uv_test_' + uv + '.pkl'
            test_ratings = sl.load_from_hadoop(test_ratings_loc, self.sc).repartition(self.num_partitions)

            #get the first content vector for results purposes
            content_path = self.directory + self.data_name +'_cv_' + self.content_vector_types[0] + '.pkl'
            content_vect = sl.load_from_hadoop(content_path, self.sc).repartition(self.num_partitions)

            # Calculate statistics about the dataset
            stats = dataset_stats.get_dataset_stats(train_ratings, test_ratings)

            for cf_pred in self.cf_predictions:

                pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cf_pred  + '.pkl'
                print 'Getting results for: ' + pred_save_loc
                preds = sl.load_from_hadoop(pred_save_loc, self.sc).repartition(self.num_partitions)

                for run in self.results_runs:
                    results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                    content_vect, self.sqlCtx, num_predictions = run, num_partitions=self.num_partitions)
                    # Merge the stats (which do not change run to run) with the results
                    results.update(stats)

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
                                + cf_pred  + '_' + str(run) + '.csv'
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
                train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)
                test_ratings_loc = self.directory + self.data_name + '_uv_test_' + uv + '.pkl'
                test_ratings = sl.load_from_hadoop(test_ratings_loc, self.sc).repartition(self.num_partitions)

                # Calculate statistics about the dataset
                stats = dataset_stats.get_dataset_stats(train_ratings, test_ratings)

                for cb_pred in self.cb_predictions:

                    pred_save_loc = self.directory + self.data_name + '_predictions_' + uv + '_' + cv + '_' \
                                + cb_pred + '.pkl'
                    print 'Getting results for: ' + pred_save_loc
                    preds = sl.load_from_hadoop(pred_save_loc, self.sc).repartition(self.num_partitions)
                    #print preds.count()

                    #if we ran the kmeans we do not need to complete both runs
                    #otherwise we do
                    if cb_pred=='cb_kmeans_100' or cb_pred=='cb_kmeans_1000':
                        if cb_pred=='cb_kmeans_1000':
                            run = 1000
                        else:
                            run = 100
                        results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                            content_vect, self.sqlCtx, num_predictions = run, num_partitions=self.num_partitions)
                        # Merge the stats (which do not change run to run) with the results
                        results.update(stats)
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
                        for run in self.results_runs:
                            results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                                                            content_vect, self.sqlCtx, num_predictions = run, num_partitions=self.num_partitions)
                            # Merge the stats (which do not change run to run) with the results
                            results.update(stats)
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

    def run_single_result(self, user_vector, content_vector, alg_type, algorithm, num_preds):

        train_ratings_loc = self.directory + self.data_name + '_uv_train_' + user_vector + '.pkl'
        train_ratings = sl.load_from_hadoop(train_ratings_loc, self.sc).repartition(self.num_partitions)
        test_ratings_loc = self.directory + self.data_name + '_uv_test_' + user_vector + '.pkl'
        test_ratings = sl.load_from_hadoop(test_ratings_loc, self.sc).repartition(self.num_partitions)

        content_path = self.directory + self.data_name +'_cv_' + content_vector + '.pkl'
        content_vect = sl.load_from_hadoop(content_path, self.sc).repartition(self.num_partitions)

        stats = dataset_stats.get_dataset_stats(train_ratings, test_ratings)

        if alg_type=='cb':
            pred_save_loc = self.directory + self.data_name + '_predictions_' + user_vector + '_' + content_vector + '_' \
                                + algorithm + '.pkl'
            results_path = self.results_directory + self.data_name + '_results_' + user_vector + '_' + content_vector + '_' \
                + algorithm  + '_' + str(num_preds) + '.csv'
        else:
            pred_save_loc = self.directory + self.data_name + '_predictions_' + user_vector +  '_' \
                                + algorithm + '.pkl'
            results_path = self.results_directory + self.data_name + '_results_' + user_vector  + '_' \
                + algorithm  + '_' + str(num_preds) + '.csv'
        print 'Getting results for: ' + pred_save_loc
        preds = sl.load_from_hadoop(pred_save_loc, self.sc).repartition(self.num_partitions)

        results = performance_metrics.get_perform_metrics(test_ratings, train_ratings, preds, \
                             content_vect, self.sqlCtx, num_predictions = num_preds, num_partitions=self.num_partitions)
        # Merge the stats (which do not change run to run) with the results
        results.update(stats)
        #add some information to the results dictionary if it gets jumbled
        results['N'] = num_preds
        results['dataset'] = self.data_name
        results['CF_CB'] = 'CB'
        results['alg_type'] = algorithm
        results['user_vector'] = user_vector
        results['content_vector'] = content_vector
        print results

        #save off the results
        print results_path
        f = open(results_path, 'w')
        f.write(str(results))
        f.close()


    def consolidate_results(self):

        dicts = []
        for file in os.listdir(self.results_directory):
            if file.startswith(self.data_name + '_results_'):
                f1 = open(self.results_directory+ file, 'r')
                my_dict = eval(f1.read())
                dicts.append(my_dict)

        run_nums = [' ']
        run_nums.extend([str(r) for r in range(0,len(dicts))])

        print 'Found ' + str(len(dicts)) + ' result sets'

        full_results_loc = self.results_directory + self.data_name + '_full_results_transpose.csv'

        with open(full_results_loc, 'wb') as ofile:
            writer = csv.writer(ofile, delimiter=',')
            writer.writerow(run_nums)
            for key in dicts[0].iterkeys():
                writer.writerow([key] + [d[key] for d in dicts])

        #this file has all the info - but to bring into pandas we want to transpose the data
        df = pd.read_csv(full_results_loc, index_col=0)
        df2 = df.transpose()

        #before saving run rmse_map to add the adjusted rmse and mae scores
        df2 = self.map_rmse_mae(df2)

        #save off the results file
        full_results_loc2 = self.results_directory + self.data_name + '_full_results.csv'
        print 'Saving: ' + full_results_loc2
        df2.to_csv(full_results_loc2, delimiter=',')

        #this data can then be brought back in with: pd.read_csv(full_results_loc2, delimiter=',', index_col=0)

    def map_rmse_mae(self, m_data):
        #this function will convert the RMSE and MAE scores to a Likert scale (1-5 where 5 is good, 1 is worse)
        #this will help compare the data accross dataset - user_vectors pairs
        #So for example the lowest RMSE for the MovieLens runs with the user_vector=ratings will all be compared together
        #this isn't the best way to do Pandas slicing so you will get warnings, but it works for now

        datasets =  list(pd.unique(m_data['dataset']))
        new_data = []
        for dataset in datasets:
            sub_data = m_data[m_data['dataset']==dataset]
            u_vects = list(pd.unique(sub_data['user_vector']))

            for vect in u_vects:
                sub_u_data = sub_data[sub_data['user_vector']==vect]
                non_zero = sub_u_data[sub_u_data['rmse']!=0]
                zero = sub_u_data[sub_u_data['rmse']==0]
                min_rmse = min(non_zero['rmse'].astype(float))
                max_rmse = max(non_zero['rmse'].astype(float))
                min_mae = min(non_zero['mae'].astype(float))
                max_mae = max(non_zero['mae'].astype(float))

                #go from the current to being 1-5
                diff_desired = 4
                diff_have_rmse = max_rmse- min_rmse
                diff_have_mae = max_mae- min_mae

                non_zero['rmse_adj'] = (max_rmse - non_zero['rmse'].astype(float))*float(diff_desired/diff_have_rmse)+1
                zero['rmse_adj'] = 3
                non_zero['mae_adj'] = (max_mae - non_zero['mae'].astype(float))*float(diff_desired/diff_have_mae)+1
                zero['mae_adj'] = 3

                if len(new_data)==0:
                    new_data = non_zero
                    new_data = new_data.append(zero)
                else:
                    new_data = new_data.append(non_zero)
                    new_data = new_data.append(zero)
        return new_data