@echo off
set SCRIPT_DIR=%~dp0
cd /d %SCRIPT_DIR%

docker build -t prchen818/collector:latest -f Dockerfile .

if %errorlevel% neq 0 (
    echo 镜像构建失败！
    exit /b %errorlevel%
) else (
    echo 镜像构建成功！
)

for /f "tokens=3" %%i in ('docker images prchen818/collector --filter "dangling=true" --format "{{.ID}}"') do (
    docker rmi -f %%i
)
echo 已删除所有悬空的 prchen818/collector 镜像。