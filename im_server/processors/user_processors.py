from manager import *

@register_processor(Command.CMD_USER_REGISTER, UserRegisterRequest, None, UserRegisterRequestSchema)
def on_user_register(context, request, reply):
	current_client_id = context.conn.id.encode("hex")
	client_id = cache_manager.get_client_id(request.username)
	if client_id:
		log.warn("username_exist|username=%s,current_client_id=%s,request_client_id=%s", request.username, client_id, current_client_id)
		return Result.ERROR_USERNAME_EXIST
	if not cache_manager.set_username(request.username, current_client_id):
		log.warn("set_username_to_cache_fail|username=%s,client_id=%s", request.username, current_client_id)
		return Result.ERROR_SERVER
	log.data("set_username|username=%s,client_id=%s", request.username, current_client_id)
	return Result.SUCCESS