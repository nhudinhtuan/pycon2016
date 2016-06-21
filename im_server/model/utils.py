def validate_protobuf_data(data, schema):
	if not data or not schema:
		return []
	return [msg_error for msg_error in schema.iter_errors(data)]