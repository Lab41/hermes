'''
Creates an interactive viewer for the hermes run data.

Use the 'bokeh serve' command to run the example by executing:
    bokeh serve hermes_run_view.py

If you haven't already you can run 'bokeh serve' by first running:
    conda install bokeh

You can then navigate in your browser to the URL:
    http://localhost:5006/hermes_run_view

'''

import numpy as np
import pandas as pd

from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, HoverTool, HBox, VBoxForm, CustomJS, Callback
from bokeh.models.widgets import Slider, Select, TextInput, CheckboxGroup
from bokeh.io import curdoc
import os

hermes_data = pd.read_csv(os.path.join(os.getcwd(), 'src/results/combined_results.csv'), delimiter=',', index_col=0)

#data_set_colors = {"Last_FM": 'green', "movielens_1m": 'black'}
#hermes_data["color"] = hermes_data["dataset"]
#hermes_data = hermes_data.replace({"color":data_set_colors})

data_set_colors = {"cf_mllib": 'green', 'cf_user': 'blue', 'cf_item': 'orange', 'cb_vect': 'purple', 'cb_kmeans_100': 'black', 'cb_kmeans_1000': 'red'}
hermes_data["color"] = hermes_data["alg_type"]
hermes_data = hermes_data.replace({"color":data_set_colors})

# Create Column Data Source that will be used by the plot
source = ColumnDataSource(data=dict(x=[], y=[], color=[], user_vector=[], content_vector=[], alg_type=[], dataset=[], num_run = []))

labels_ds = ColumnDataSource(data=dict(labels=[], active = []))


axis_map = {
    "Mean Absolute Error": "mae",
    "Average Rank": "avg_mean_rank",
    "User Coverage": "user_coverage",
    "Precision @ N": "pred_n",
    "Catalog Coverage": "cat_coverage",
    "Prediction Coverage": "pred_coverage",
    "Novelty": "novelty",
    "Root Mean Square Error": "rmse",
    "Item Coverage": "item_coverage",
    "ILS (diversity)": "ils",
    "Category Diversity": "cat_diversity",
    "Serendipity": "serendipity",
    "Content Serendipity": "content_serendipity",
}

x_axis = Select(title="X Axis", options=sorted(axis_map.keys()), value="Precision @ N")
y_axis = Select(title="Y Axis", options=sorted(axis_map.keys()), value="Item Coverage")
num_preds = Select(title="N", value='1000', options=['All', '100', '1000'])
alg_type = Select(title = "Recommender Type", value='All', options = ['All', 'All CF', 'All CB', 'cf_mllib', 'cf_user', 'cf_item', 'cb_vect', 'cb_kmeans'])


label_source = ColumnDataSource(data=dict(labels=['other', 'hello', 'there']))


callback1 = CustomJS(args=dict(source=source), code="""
        var f = source.get('dataset')
        if f=='movie_lens1m'{
            #label_source.data= dict(labels = ['ratings', 'pos_ratings', 'ratings_to_interact'])
            s2.set("options", ['ratings', 'pos_ratings', 'ratings_to_interact']
        }
        else if f=='Last_FM'{
            #label_source.data= dict(labels = ['num_plays', 'any_interact', 'num_plays_log'])
            s2.set("options", ['num_plays', 'any_interact', 'num_plays_log']
        }
        else {
            #label_source.data= dict(labels = ['other', 'hello', 'there'])
            s2.set("options", ['other', 'hello', 'there']
        }
        s2.trigger('change');
    """)

dataset_type = Select(title = "Dataset", value='movielens_1m', options = ['All', 'All MovieLens', 'movielens_1m', 'Last_FM', 'git'], callback=callback1)

checkbox_button_group = Select(value="", title = "User Vector")

callback1.args["s2"] = checkbox_button_group

labels_list = []
active_list = []

#checkbox_button_group.set('labels')
#checkbox_button_group.trigger('change')

#callback1.args['s2'] = checkbox_button_group


hover = HoverTool(tooltips=[
    ("Dataset","@dataset"),
    ("User Vector","@user_vector"),
    ("Content Vector", "@content_vector"),
    ("Algorithm", "@alg_type"),
    ("Num_preds", "@num_run")
])

p = Figure(plot_height=600, plot_width=800, title="", toolbar_location=None, tools=[hover])
p.circle(x="x", y="y", source=source, size=7, color="color", line_color=None)

def select_run():
    num_preds_val = num_preds.value
    alg_type_val = alg_type.value
    dataset_val = dataset_type.value
#     director_val = director.value.strip()
#     cast_val = cast.value.strip()
    selected = hermes_data
#     selected = movies[
#         (movies.Reviews >= reviews.value) &
#         (movies.BoxOffice >= (boxoffice.value * 1e6)) &
#         (movies.Year >= min_year.value) &
#         (movies.Year <= max_year.value) &
#         (movies.Oscars >= oscars.value)
#     ]

    if num_preds_val != 'All':
        selected = selected[selected['N']==int(num_preds_val)]

    if alg_type_val != "All":
        if alg_type_val == "All CF":
            selected = selected[selected['alg_type'].str.contains('cf')]
        elif alg_type_val == "All CB":
            selected = selected[selected['alg_type'].str.contains('cb')]
        else:
            selected = selected[selected['alg_type'].str.contains(alg_type_val)]
    if (dataset_val!='All'):
        if dataset_val =='All MovieLens':
            selected = selected[selected['dataset'].str.contains('movielens')]
        else:
            selected = selected[selected['dataset'].str.contains(dataset_val)]

#     if (director_val != ""):
#         selected = selected[selected.Director.str.contains(director_val)==True]
#     if (cast_val != ""):
#         selected = selected[selected.Cast.str.contains(cast_val)==True]
    return selected

def update(attrname, old, new):
    global labels_list, active_list
    df = select_run()
    x_name = axis_map[x_axis.value]
    y_name = axis_map[y_axis.value]

    p.xaxis.axis_label = x_axis.value
    p.yaxis.axis_label = y_axis.value
    p.title = "%d runs selected" % len(df)
    source.data = dict(
        x=df[x_name],
        y=df[y_name],
        color=df["color"],
        user_vector=df["user_vector"],
        content_vector=df["content_vector"],
        alg_type=df["alg_type"],
        dataset = df["dataset"],
        num_run = df["N"],
        #alpha = df["alpha"],
    )
    # #update labels and
    # if  df["dataset"].any()=='movielens_1m':
    #     labels_list= ['ratings', 'pos_ratings', 'ratings_to_interact'],
    #     active_list =[1,1,1]
    #
    #
    # elif  df["dataset"].any()=='Last_FM':
    #     labels_list= ['num_plays', 'any_interact', 'num_plays_log'],
    #     active_list =[1,1,1]
    #
    # else:
    #
    #     labels_list = [],
    #     active_list = []


controls = [alg_type, dataset_type,  num_preds, x_axis, y_axis]
for control in controls:
    control.on_change('value', update)

inputs = HBox(VBoxForm(*controls), width=300)

update(None, None, None) # initial load of the data

curdoc().add_root(HBox(inputs, p, width=1100))

