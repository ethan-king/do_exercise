import datetime

SESSION_SCHEMA = {
	'user_id': str,
	'tutorial_id': int,
	'session_start_at': str,
	'session_end_at': str,
}

TAGS_SCHEMA = {
	'id': int,
	'name': str,
	'description': str,
	'tag_type': str
}

TUTORIALS_SCHEMA = {
	'tutorial_id': int,
	'title': str,
	'slug': str,
	'description': str,
	'created_at': str,
	'tag_id': int
}