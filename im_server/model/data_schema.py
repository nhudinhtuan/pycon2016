from constant import *

UInt8Schema = {"type": "integer", "minimum": 0, "maximum": TYPE_UINT8_MAX}
UInt16Schema = {"type": "integer", "minimum": 0, "maximum": TYPE_UINT16_MAX}
UInt32Schema = {"type": "integer", "minimum": 0, "maximum": TYPE_UINT32_MAX}
Int32Schema = {"type": "integer", "minimum": TYPE_INT32_MIN, "maximum": TYPE_INT32_MAX}
UInt64Schema = {"type": "integer", "minimum": 0, "maximum": TYPE_UINT64_MAX}
UFloatSchema = {"type": "number", "minimum": 0}
StringSchema = {"type": "string"}
BoolSchema = {"type": "boolean"}
IdSchema = {"type": "integer", "minimum": 1, "maximum": TYPE_UINT64_MAX}

UserIdSchema = {"type": "string", "maxLength": 100}

MessageInfoSchema = {
	"type": "object",
	"properties": {
		"from_id": UserIdSchema,
		"content": {"type": "string", "maxLength": 500}
	},
	"required": ["content"]
}

# CMD_USER_REGISTER

UserRegisterRequestSchema = {
	"type": "object",
	"properties": {
		"username": {"type": "string", "maxLength": 100}
	},
	"required": ["username"]
}

# CMD_MESSAGE_SEND

MessageSendRequestSchema = {
	"type": "object",
	"properties": {
		"targets": {
			"type": "array",
			"minItems": 1,
			"items": UserIdSchema
		},
		"message": MessageInfoSchema
	},
	"required": ["targets", "message"]
}