from manager import *

@register_processor(Command.CMD_MESSAGE_SEND, MessageSendRequest, None, MessageSendRequestSchema)
def on_message_send(context, request, reply):
	current_client_id = context.conn.id.encode("hex")
	from_username = cache_manager.get_username(current_client_id)
	if not from_username:
		log.warn("send_message_before_register|client_id=%s", current_client_id)
		return Result.ERROR_FORBIDDEN

	client_id_dict = cache_manager.get_client_id_list(request.targets)
	if len(client_id_dict) == 0:
		log.warn("username_not_exist|request=%s", pb_to_str(request))
		return Result.ERROR_USERNAME_NOT_EXIST

	notify_header = PacketHeader()
	notify_header.command = Command.CMD_MESSAGE_NOTIFY
	notify_header.timestamp = get_timestamp()

	notify_request = MessageNotifyRequest()
	notify_request.message.CopyFrom(request.message)
	notify_request.message.from_id = from_username

	notify_packet = context.processor.construct_reply_packet(Result.SUCCESS, notify_header, notify_request)

	for username, client_id in client_id_dict.iteritems():
		if not context.processor.send_packet(client_id.decode("hex"), notify_packet):
			log.warn("notify_message_fail|from_username=%s,from_client_id=%s,to_username=%s,to_client_id=%s,message=%s", from_username, current_client_id, username, client_id, request.message.content)
			return Result.ERROR_SERVER
		log.data("notify_message|from_username=%s,to_username=%s,message=%s", from_username, username, request.message.content)

	return Result.SUCCESS