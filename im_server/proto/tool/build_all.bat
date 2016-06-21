@cd /D "%~dp0/.."
@del /F /Q "*.pb.h"
@del /F /Q "*.pb.cpp"
@del /F /Q "*.py"
@cd . > "__init__.py"
@FOR %%I IN ("*.proto") DO @(
	@echo %%I
	@"%~dp0/protoc.exe" --cpp_out=. --python_out=. "%%I"
)
@echo Build protobuf files over.
@pause