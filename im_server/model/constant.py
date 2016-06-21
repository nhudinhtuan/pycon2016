from gtcp.pbutils import pb_enum_to_class, pb_enum_values_to_class
from im_server.proto.DemoData_pb2 import Constant as DemoConstant

TYPE_UINT8_MAX = 255
TYPE_UINT16_MAX = 65535
TYPE_UINT32_MAX = 4294967295
TYPE_INT32_MIN = -2147483648
TYPE_INT32_MAX = 2147483647
TYPE_UINT64_MAX = 18446744073709551615

Command = pb_enum_to_class(DemoConstant, 'Command')
Result = pb_enum_to_class(DemoConstant, 'Result')
