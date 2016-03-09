'''
Creates an interactive viewer for the hermes run data.

Use the 'bokeh serve' command to run the example by executing:
    bokeh serve src/results/hermes_run_view.py

If you haven't already you can run 'bokeh serve' by first running:
    conda install bokeh

You can then navigate in your browser to the URL:
    http://localhost:5006/hermes_run_view

'''

import numpy as np
import pandas as pd

from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, HoverTool, HBox, VBoxForm, CustomJS, Callback
from bokeh.models.widgets import Slider, Select, TextInput, CheckboxGroup, Button
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
    "Root Mean Square Error": "rmse",
    "Adjusted Root Mean Square Error": "rmse_adj",
    "Mean Absolute Error": "mae",
    "Adjusted Mean Absolute Error": "mae_adj",
    "Precision @ N": "pred_n",
    "Item Coverage": "item_coverage",
    "User Coverage": "user_coverage",
    "Catalog Coverage": "cat_coverage",
    "Prediction Coverage": "pred_coverage",
    "Category Diversity": "cat_diversity",
    "ILS (diversity)": "ils",
    "Serendipity": "serendipity",
    "Content Serendipity": "content_serendipity",
    "Novelty": "novelty",
    "Average Rank": "avg_mean_rank",
    "Dataset Density": "density",
}

x_axis = Select(title="X Axis", options=axis_map.keys(), value="Precision @ N")
y_axis = Select(title="Y Axis", options=axis_map.keys(), value="Item Coverage")
num_preds = Select(title="N", value='1000', options=['All', '100', '1000'])
alg_type = Select(title = "Recommender Type", value='All', options = ['All', 'All CF', 'All CB', 'cf_mllib', 'cf_user', 'cf_item', 'cb_vect', 'cb_kmeans'])

u_button = Button(label="User Vector")
c_button = Button(label="Content Vector")

#initialize to movielens
labels = ['pos_ratings', 'ratings', 'ratings_to_interact']
c_labels = ["genre", "tags"]
c_active = [0,1]

dataset_names = list(pd.unique(hermes_data['dataset']))
initial_dataset_names = ["All", "All Movielens"]
initial_dataset_names.extend(dataset_names)
dataset_type = Select(title = "Dataset", value='movielens_1m', options = initial_dataset_names)

checkbox_button_group = CheckboxGroup(labels=labels, active = [0,1,2])
checkbox_button_group_content = CheckboxGroup(labels=c_labels, active = c_active)


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

    #this sometimes currently goes through two iterations, the first has not fully updated the list
    #with the try except then we are able to set the list correctly and not error out
    try:
        u_vects = list(pd.unique(selected['user_vector']))
        active_u_vects = checkbox_button_group.active

        active_labels = []
        for a in active_u_vects:
            active_labels.append(u_vects[int(a)])

        selected = selected[selected['user_vector'].isin(active_labels)]

        c_vects = list(pd.unique(selected['content_vector']))
        active_c_vects = checkbox_button_group_content.active

        active_c_labels = []
        for a in active_c_vects:
            active_c_labels.append(c_vects[int(a)])
        selected = selected[selected['content_vector'].isin(active_c_labels)]
    except:
        pass

    return selected

def update(attrname, old, new):
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


def update_dataset(attrname, old, new):

    #set up a dataframe for this type of dataset to get the user vectors
    dataset_val = dataset_type.value
    if (dataset_val!='All'):
        if dataset_val =='All MovieLens':
            df2 = hermes_data[hermes_data['dataset'].str.contains('movielens')]
        else:
            df2 = hermes_data[hermes_data['dataset'].str.contains(dataset_val)]
    else:
        df2 = hermes_data

    #when changing the dataset reset the user vector labels
    labels_list = list(pd.unique(df2['user_vector']))
    active_list = range(0, len(labels_list))

    checkbox_button_group.labels = labels_list
    checkbox_button_group.active = active_list

    #also change the content vector types
    c_labels_list = list(pd.unique(df2['content_vector']))
    c_active_list = range(0, len(c_labels_list))

    checkbox_button_group_content.labels = c_labels_list
    checkbox_button_group_content.active = c_active_list

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


def checkbox_handler(active):
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



controls = [alg_type,  num_preds, x_axis, y_axis, dataset_type, u_button, checkbox_button_group, c_button, checkbox_button_group_content]
for control in controls:
    if control==dataset_type:
        control.on_change('value', update_dataset)
    else:
        control.on_change('value', update)

checkbox_button_group.on_click(checkbox_handler)
checkbox_button_group_content.on_click(checkbox_handler)

inputs = HBox(VBoxForm(*controls), width=300)

update(None, None, None) # initial load of the data

curdoc().add_root(HBox(inputs, p, width=1100))

