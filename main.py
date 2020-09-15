"""
This is my code for the Digital Ocean Data Engineer Interview Assignment.
First the data across the three tables will be modeled with sessions as the fact table and tutorials and tags as
dimension tables.


https://dse-interview.sfo2.digitaloceanspaces.com/DigitalOcean_Data_Science_Assignment.zip
"""
import io
from zipfile import ZipFile
import re
import os

import requests
import pandas as pd
import plotly.express as px

from schemas import *

## DASH
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

app = dash.Dash(__name__)
app.title = 'Digital Ocean Assessment'
server = app.server

DATA_URL = 'https://dse-interview.sfo2.digitaloceanspaces.com/DigitalOcean_Data_Science_Assignment.zip'
LOCAL_PATH_ZIP = 'DigitalOcean_Data_Science_Assignment.zip'
OUTPUT_PATH = 'output'

# check if the /output folder exists, if not, create it
if not os.path.exists('output'):
    os.makedirs('output')

def getZipData():
	"""
	fetches data from the repo and returns the csvs as Dataframes in a dictionary keyed by filenames
	:param try_local: try to load the locally saved data, or else load from repo
	:return: a dictionary containing the three datasets
	"""
	def openLocalOrRepo():
		if os.path.exists(LOCAL_PATH_ZIP):
			print('Opening local zip file')
			return ZipFile(LOCAL_PATH_ZIP)
		else:
			print('Downloading data from repo')
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
	zipped = getZipData()
	print('Unpacking tables into dictionary')
	dfs = {}
	for info in zipped.filelist:
		if re.match(r'^.+/(sessions.csv)$', info.filename):
			print('sessions')
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=SESSION_SCHEMA, parse_dates=['session_start_at', 'session_end_at'], index_col=['tutorial_id'])
			df['session_duration'] = df['session_end_at'] - df['session_start_at']
			dfs['sessions'] = df
		elif re.match(r'^.+/(tutorials.csv)$', info.filename):
			print('tutorials')
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=TUTORIALS_SCHEMA, parse_dates=['created_at'], index_col=['tutorial_id'])
			df = df.drop(['slug', 'description', 'created_at'], axis=1)
			dfs['tutorials'] = df
		elif re.match(r'^.+/(tags.csv)$', info.filename):
			print('tags')
			df = pd.read_csv(io.BytesIO(zipped.read(info.filename)), dtype=TAGS_SCHEMA,)
			df = df.drop(['description', 'tag_type'], axis=1)
			dfs['tags'] = df
		else:
			continue
	return dfs

def getSliceByYearMonthDay(df, dateCol, year, month, day):
	return df[(df[dateCol].dt.year == year) & (df[dateCol].dt.month == month) & (df[dateCol].dt.day == day)]
def getSliceByYearMonth(df, dateCol, year, month):
	return df[(df[dateCol].dt.year == year) & (df[dateCol].dt.month == month)]
def getSliceByYear(df, dateCol, year):
	return df[(df[dateCol].dt.year == year)]

dataDict = unzipToDict()

timeGroupDaily = pd.Grouper(key='session_end_at', freq='d')

DATE_MIN = dataDict['sessions']['session_start_at'].min()#.to_pydatetime()
DATE_MAX = dataDict['sessions']['session_end_at'].max()#.to_pydatetime()

################ Plotly
# Layout
app.layout = html.Div(
	[
		html.Div(
			[
				html.H3('Daily Views and Avg Session Length Date Range:', style={'paddingRight': '30px', 'font-family':'sans-serif', 'color':'white'}),
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
					style={'fontSize':24, 'marginLeft':'30px', 'marginTop': '5px'}
				)
			], style={'display': 'inline-block', 'verticalAlign': 'top', 'background-color': '#006aff', 'width': '100%'}
		),
		html.Div([]),
		html.Div(
			[
				html.Div(
					[
						dcc.Graph(
							id='view_count_graph',
						),
						dcc.Graph(
							id='time_spent_graph'
						),
					], style= {'display': 'inline-block', 'width': '49%'}
				),
				html.Div(
					[
						dcc.Graph(id='user_avg_session_graph')
					], style= {'display': 'inline-block', 'width': '49%'}
				)
			], #style={'display': 'inline-block'}
		),
		html.Div(
			[
				html.H3('User View Count & Top 10 List Date Range:', style={'paddingRight': '30px', 'font-family':'sans-serif', 'color':'white'}),
				dcc.DatePickerRange(
							id='date_picker_2',
							min_date_allowed=DATE_MIN,
							max_date_allowed=DATE_MAX,
							start_date=datetime.datetime(2019,3,1),
							end_date=DATE_MAX
				),
				html.Button(
					id='submit_button_2',
					n_clicks=0,
					children='Submit',
					style={'fontSize':24, 'marginLeft':'30px', 'marginTop': '5px'}
				)
			], style={'display': 'inline-block', 'background-color': '#006aff', 'width': '100%'} #
		),
		html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='top_tags'),
					], style= {'display': 'inline-block', 'width': '59%'}
				),
				html.Div(
					[
						dcc.Graph(id='user_view_count_graph')
					], style={'display': 'inline-block', 'width': '39%'}
				),
				html.Div(
					[
						# dcc.Graph(
						# 	id='top_tags'
						# ),
						dcc.Graph(
							id='top_tutorials',
						),
					], style= {'display': 'inline-block', 'width': '100%'}
				),
			], #style={'display': 'inline-block'}
		),
		html.Div(id='none', children=[], style={'display': 'none'})
	]
)

# update dashboard
@app.callback(
	[
		Output('view_count_graph', 'figure'),
		Output('time_spent_graph', 'figure'),
		Output('user_avg_session_graph', 'figure')
	],
	[Input('submit_button_1', 'n_clicks'),],
	[
		State('date_picker_1', 'start_date'),
		State('date_picker_1', 'end_date'),
	]
)
def update_graph_view_count(none1, start_date_1, end_date_1,):
	start_date_ft = start_date_1[:10]
	end_date_ft = end_date_1[:10]
	# time and count of tutorials
	data = dataDict['sessions']
	data = data[(data['session_end_at']>= start_date_1) & (data['session_end_at']<= end_date_1)]

	tutorialCt = data.groupby(timeGroupDaily)[['session_duration', 'user_id']].agg(
		{'session_duration': ['count', 'sum'], 'user_id': pd.Series.nunique})
	tutorialCt['session_user', 'avg'] = tutorialCt['session_duration', 'sum'] / tutorialCt['user_id', 'nunique']
	tutorialCt['session_user', 'sessions_per_user'] = tutorialCt['session_duration', 'count'] / tutorialCt[
		'user_id', 'nunique']
	df = tutorialCt.reset_index()
	dailyViewTrace = []
	dailyViewTrace.append({'x': df['session_end_at'], 'y': df['session_duration', 'count'], 'name': 'Number of Sessions'})
	dailyViewTrace.append({'x': df['session_end_at'], 'y': df['user_id', 'nunique'], 'name':'Number of Users'})
	tutorialViews = {
		'data': dailyViewTrace,
		'layout': {'title': "Daily Tutorial View Count", 'height': 300, 'showlegend': False}
	}

	dailyTimeTrace = []
	dailyTimeTrace.append({'x': df['session_end_at'], 'y': df['session_user', 'avg'].dt.seconds/60, 'name': 'Average Session'})
	tutorialTime = {
		'data': dailyTimeTrace,
		'layout': {'title': 'Daily Average User Session Length (minutes)', 'height': 300, 'showlegend': False}
	}
	data['session_duration_seconds'] = data['session_duration'].dt.seconds/60
	userAvgSessionDuration = data.groupby('user_id')[['session_duration_seconds']].mean()
	userAvgSessionDuration = userAvgSessionDuration.rename({'session_duration_seconds': 'session_duration_avg'}, axis=1)
	userAvgSessionDurationTrace = [{'x': userAvgSessionDuration['session_duration_avg'], 'type': 'histogram', 'histnorm': 'percent', 'xbins':{'start':0, 'end':60, 'size':1}}]
	userAvgSessionDurationHist = {
		'data': userAvgSessionDurationTrace,
		'layout': {'title': 'User Average Session Duration', 'height': 600, 'yaxis': {'title': {'text': 'percentage'}}}
	}

	# try:
	# 	print('Saving tutorial_daily_views to output directory')
	# 	pd.DataFrame.to_csv(df, os.path.join(OUTPUT_PATH, f'tutorial_daily_views_{start_date_ft}_{end_date_ft}.csv'))
	# 	print('Saving user_avg_session to output directory')
	# 	pd.DataFrame.to_csv(userAvgSessionDuration, os.path.join(OUTPUT_PATH, f'user_avg_session_{start_date_ft}_{end_date_ft}.csv'))
	# except FileNotFoundError:
	# 	print('Create the directory "output"')

	return tutorialViews, tutorialTime, userAvgSessionDurationHist

# update dashboard
@app.callback(
	[
		Output('user_view_count_graph', 'figure'),
		Output('top_tags', 'figure'),
		Output('top_tutorials', 'figure'),
	],
	[Input('submit_button_2', 'n_clicks')],
	[
		State('date_picker_2', 'start_date'),
		State('date_picker_2', 'end_date'),
	]
)
def update_user_view_ct_top_10(none, start_date_2, end_date_2):
	start_date_ft = start_date_2[:10]
	end_date_ft = end_date_2[:10]
	df2	= dataDict['sessions'].reset_index()

	df2 = df2[(df2['session_end_at']>= start_date_2) & (df2['session_end_at']<= end_date_2)]
	userViewCountData = df2.groupby(['user_id'])[['tutorial_id']].count()

	userViewCountTrace= [{'x': userViewCountData['tutorial_id'], 'type': 'histogram', 'histnorm': 'percent', 'xbins':{'start':0, 'end':50, 'size':1}}] #
	userViewCount = {
		'data': userViewCountTrace,
		'layout': {'title': {'text': 'User View Count Distribution', 'xanchor': 'left', 'x':.15}, 'height': 300, 'yaxis': {'title': {'text': 'percentage'}}}
	}
	# try:
	# 	print('Saving user_view_count data to output directory')
	# 	userViewCountData.to_csv(os.path.join(OUTPUT_PATH, f'user_view_count_{start_date_ft}_{end_date_ft}.csv'))
	# except FileNotFoundError:
	# 	print('Create the directory "output"')


	df3 = dataDict['sessions']
	df3 = df3[(df3['session_end_at'] >= start_date_2) & (df3['session_end_at'] <= end_date_2)]
	df3_tags = df3.join(dataDict['tutorials'], how='left').merge(dataDict['tags'], left_on='tag_id',
																			 right_on='id', how='left')
	dailyTags = df3_tags.groupby([timeGroupDaily, 'name'])[['tag_id']].count().sort_values(
		by=['session_end_at', 'tag_id'], ascending=[True, False])  # .sort_index(level=[0,1], sort_remaining=False)
	dailyTags = dailyTags.rename({'tag_id': 'count'}, axis=1)
	dailyTags10 = dailyTags.groupby('session_end_at').head(10).reset_index()
	top_tag_fig = px.bar(dailyTags10, x="session_end_at", y="count", color="name", title="Daily Top 10 Tags", height=300)

	dailyTutorials = df3_tags.groupby([timeGroupDaily, 'title'])[['tag_id']].count().sort_values(
		by=['session_end_at', 'tag_id'], ascending=[True, False])
	dailyTutorials = dailyTutorials.rename({'tag_id': 'count'}, axis=1)
	dailyTutorials10 = dailyTutorials.groupby('session_end_at').head(10).reset_index()
	top_tutorial_fig = px.bar(dailyTutorials10, x="session_end_at", y="count", color="title", title="Daily Top 10 Tutorials", height=400)

	# try:
	# 	print('Saving daily_top_tags to output directory')
	# 	dailyTags.to_csv(os.path.join(OUTPUT_PATH, f'daily_top_tags_{start_date_ft}_{end_date_ft}.csv'))
	# 	print('Saving daily_top_tutorials to output directory')
	# 	dailyTutorials.to_csv(os.path.join(OUTPUT_PATH, f'daily_top_tutorials_{start_date_ft}_{end_date_ft}.csv'))
	# except FileNotFoundError:
	# 	print('Create the directory "output"')

	return userViewCount, top_tag_fig, top_tutorial_fig

if __name__ == '__main__':
	# app.run_server()
	print('ready')
