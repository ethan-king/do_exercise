"""
This is my code for the Digital Ocean Data Engineer Interview Assignment.
First the data across the three tables will be modeled with sessions as the fact table and tutorials and tags as
dimension tables.
- How much time are users spending on tutorial pages per day? How many tutorials are they viewing a day?
- How often do individual users view tutorials? Daily? A few days a month?
- What tutorial topics (tags) are most popular per day?
- What tutorials are most popular per day?

https://dse-interview.sfo2.digitaloceanspaces.com/DigitalOcean_Data_Science_Assignment.zip
"""
import io
from zipfile import ZipFile
import re
import os
import datetime

import requests
import pandas as pd
import numpy as np
import plotly as py
import matplotlib.pyplot as plt

from schemas import *


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


## DASH
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

app = dash.Dash(__name__)
app.title = 'Digital Ocean Assessment'
server = app.server



def getData():
	"""
	fetches data from the repo and returns the csvs as Dataframes in a dictionary keyed by filenames
	:param try_local: try to load the locally saved data, or else load from repo
	:return: a dictionary containing the three datasets
	"""
	def openLocalOrRepo():
		LOCAL_PATH = 'DigitalOcean_Data_Science_Assignment.zip'
		if os.path.exists(LOCAL_PATH):
			print('opening local zip file')
			return ZipFile(LOCAL_PATH)
		else:
			print('Downloading data from repo')
			DATA_URL = 'https://dse-interview.sfo2.digitaloceanspaces.com/DigitalOcean_Data_Science_Assignment.zip'
			data = requests.get(DATA_URL)
			zipped = ZipFile(io.BytesIO(data.content))
			return zipped
	return openLocalOrRepo()

def unzipToDict():
	"""
	unpacks zipped tables
	:param zip: a ZipFile object containing the repo data
	:return: dictionary with unpacked tables
	"""
	zipped = getData()
	print('Unpacking tables into dictionary')
	dfs = {}
	for info in zipped.filelist:
		if re.match(r'^.+/(sessions.csv)$', info.filename):
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=SESSION_SCHEMA, parse_dates=['session_start_at', 'session_end_at'], index_col=['tutorial_id'])
			df['session_duration'] = df['session_end_at'] - df['session_start_at']
			dfs['sessions'] = df
		elif re.match(r'^.+/(tutorials.csv)$', info.filename):
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=TUTORIALS_SCHEMA, parse_dates=['created_at'], index_col=['tutorial_id'])
			df = df.drop(['slug', 'description', 'created_at'], axis=1)
			dfs['tutorials'] = df
		elif re.match(r'^.+/(tags.csv)$', info.filename):
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=TAGS_SCHEMA,)
			df = df.drop(['description', 'tag_type'], axis=1)
			dfs['tags'] = df
		else:
			continue
	return dfs

def getSliceByMonthYear(df, dateCol, year, month):
	return df[(df[dateCol].dt.year == year) & (df[dateCol].dt.month == month)]
def getSliceByYear(df, dateCol, year):
	return df[(df[dateCol].dt.year == year)]

dataDict = unzipToDict()

timeGroupDaily = pd.Grouper(key='session_end_at', freq='d')

DATE_MIN = dataDict['sessions']['session_start_at'].min()#.to_pydatetime()
DATE_MAX = dataDict['sessions']['session_end_at'].max()#.to_pydatetime()

# time and count of tutorials
tutorialCt = dataDict['sessions'].groupby(timeGroupDaily)[['session_duration', 'user_id']].agg(
	{'session_duration': ['count', 'sum'], 'user_id': 'count'})
tutorialCt['session_user', 'avg'] = tutorialCt['session_duration', 'sum'] / tutorialCt['user_id', 'count']

# # how much time are users spending on tutorial pages per day? how many tutorials are they viewing a day?
# userViews = dataDict['sessions'].pivot_table(index='user_id', columns=[pd.Grouper(key='session_end_at', freq='d')], values='session_duration', aggfunc='count')
# userViewsGrp = dataDict['sessions'].groupby(pd.Grouper(key='session_end_at', freq='M')).count()

# get a month slice of the dataset by filtering session_end_at
monthlySlice = getSliceByMonthYear(dataDict['sessions'], 'session_end_at', 2019, 2)
dailyViews = monthlySlice.groupby(['user_id'])[['session_end_at']].count() # use 30 as a standard normalizing factor

# plt.hist(dailyViews['session_end_at'], bins=np.arange(1, 10, 1), weights=np.ones(dailyViews.index.shape[0]) / dailyViews.index.shape[0])
# plt.gca().set_xlim([0, 2])
# plt.show()

# # get a year slice
# yearlySlice = getSliceByYear(dataDict['sessions'], 'session_end_at', 2019)
# monthlyViews = yearlySlice.groupby(['user_id'])[['session_end_at']].count() / 12
#
# plt.hist(monthlyViews['session_end_at'], bins=np.arange(0, 4, 0.25))
# # plt.gca().set_xlim([0, 10])
# plt.show()
#
# # most popular tags by day
# # sessions: get count of how many times each tagID accessed each day
# monthlyTags = monthlySlice.join(dataDict['tutorials'], how ='left').merge(dataDict['tags'], left_on='tag_id', right_on='id', how='left')
# dailyTags = monthlyTags.groupby([timeGroupDaily, 'name'])[['tag_id']].count().sort_values(by=['session_end_at', 'tag_id'], ascending=[True, False])#.sort_index(level=[0,1], sort_remaining=False)
# dailyTags = dailyTags.groupby('session_end_at').head(10)
#
# # most popular tutorials per day
# dailyTutorials = monthlyTags.groupby([timeGroupDaily, 'title'])[['user_id']].count().sort_values(by=['session_end_at', 'user_id'], ascending=[True, False])
# dailyTutorials = dailyTutorials.groupby('session_end_at').head(10)


################ Plotly
# Layout

app.layout = html.Div(
	[
		html.Div(
			[
				html.H3('Date Range:', style={'paddingRight': '30px'}),
				dcc.DatePickerRange(
							id='date_picker_1',
							min_date_allowed=DATE_MIN,
							max_date_allowed=DATE_MAX,
							start_date=DATE_MIN,
							end_date=DATE_MAX
				),
				html.Button(
					id='submit_button_1',
					n_clicks=0,
					children='Submit',
					#style={'fontSize':24, 'marginLeft':'30px', 'marginTop': '5px'}
				)


			], style={'display': 'inline-block', 'verticalAlign': 'top', 'width': '35%'}
		),
		dcc.Graph(
			id='view_count_graph',
			figure =
				{
				'data': [{'x':[1,2], 'y':[3,1]}],
				'layout': {'title':'Default Title'}
				}
		),
		dcc.Graph(
			id='time_spent_graph'
		),
		html.Div(
			[

			], style={'display': 'inline-block'}
		),
		html.Div(id='none', children=[], style={'display': 'none'})
	]
)
# update dashboard
@app.callback(
	[
		Output('view_count_graph', 'figure'),
		Output('time_spent_graph', 'figure')
	],
	[
		Input('submit_button_1', 'n_clicks'),
	],
	[
		State('date_picker_1', 'start_date'),
		State('date_picker_1', 'end_date')
	]
)
def update_graph_view_count(none, start_date, end_date):
	dailyViewTrace = []
	df = tutorialCt.reset_index()
	df = df[(df['session_end_at']>= start_date) & (df['session_end_at']<= end_date)]
	dailyViewTrace.append({'x': df['session_end_at'], 'y': df['session_duration', 'count']})
	tutorialViews = {
		'data': dailyViewTrace,
		'layout': {'title': "Daily Tutorial View Count", 'height': 300}
	}

	dailyTimeTrace = []
	dailyTimeTrace.append({'x': df['session_end_at'], 'y': df['session_user', 'avg'].dt.seconds/60})
	tutorialTime = {
		'data': dailyTimeTrace,
		'layout': {'title': 'Daily Average User Session Length (minutes)', 'height': 300}
	}
	return tutorialViews, tutorialTime


def update_graph_time_spent_per_day(none,):
	pass


if __name__ == '__main__':
	# app.run_server()
	print('ready')