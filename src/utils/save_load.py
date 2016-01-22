import csv
import gzip

def save_vector(vector, output_fname):
    """
    Save the any type of vector for future use.
    This could be ratings, predictions or the content vector
    Results need to be collected to the local history before being read out

    Args:
        vector: either user ratings, predictions or the content vector
        output_fname (str): Local file path to store the vector
    """
    if output_fname.endswith('.gz'):
        output_file = gzip.open(output_fname, 'w')
    else:
        output_file = open(output_fname, 'w')
    csv_writer = csv.writer(output_file, delimiter=';')
    for v in vector:
        csv_writer.writerow(v)
    output_file.close()

def load_ratings(input_fname):
    """
    Loads the rating or predicted rating arrays into the format of int(user_id), int(item_id), float(rating)
    The ratings array can then be put into spark by using sc.parallelize()
    If you would then like a queriable ratings vector you would follow something similar to the following
        ratings_sc = sc.parallelize(ratings)
        fields = [StructField("user", LongType(),True),StructField("item", LongType(), True),\
              StructField("rating", FloatType(), True) ]
        schema = StructType(fields)
        ratings_df = sqlCtx.createDataFrame(ratings_sc, schema)
        ratings_df.registerTempTable("ratings")

    Args:
        input_fname (str): Local file path where the vector is stored
    Returns:
        ratings: array of user ratings or predicted rating

    """

    ratings = []

    if input_fname.endswith('.gz'):
        input_file = gzip.open(input_fname, 'rb')
    else:
        input_file = open(input_fname, 'rb')
    csv_reader = csv.reader(input_file, delimiter=';')

    for line in csv_reader:
        ratings.append((int(line[0]), int(line[1]), float(line[2])))
    return ratings

def load_content_vector(input_fname):
    """
    Loads the content vector array into the format of int(item_id), array[0, 1, 0, ....., 0.777773, 0]
    The content vector array can then be put into spark by using sc.parallelize()

    Args:
        input_fname (str): Local file path where the vector is stored
    Returns:
        content_vector: array of the content vector

    """

    content_vector = []

    if input_fname.endswith('.gz'):
        input_file = gzip.open(input_fname, 'rb')
    else:
        input_file = open(input_fname, 'rb')
    csv_reader = csv.reader(input_file, delimiter=';')

    for line in csv_reader:
        item = int(line[0])
        content1 = line[1].strip("[]")
        content = [float(i) for i in str.split(content1, ' ')]
        content_vector.append((item, content))
    return content_vector

def save_uv_to_hadoop(vector, output_name):
    vector.map(lambda x: ','.join(map(str,x))).saveAsTextFile(output_name)

def load_uv_from_hadoop(input_name, sc, num_partitions=20):
    uv = sc.textFile(input_name).map(parseText)\
        .repartition(num_partitions)
    return uv

def parseText(row):
    row = row.split(',')
    return (int(row[0]), int(row[1]), float(row[2]))

def save_cv_to_hadoop(vector, output_name):
    vector.saveAsPickleFile(output_name)

def load_cv_from_hadoop(input_name,sc,  num_partitions=20):
    cv = sc.pickleFile(input_name).repartition(num_partitions)
    return cv