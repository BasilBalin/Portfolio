@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo.
echo ========================================
echo    Запуск DictKeeper Bot
echo ========================================
echo.

REM Проверяем существование виртуального окружения
if not exist "venv\" (
    echo [1/4] Создание виртуального окружения...
    python -m venv venv
    echo Виртуальное окружение создано.
) else (
    echo Виртуальное окружение уже существует.
)

echo.
echo [2/4] Активация виртуального окружения...
call venv\Scripts\activate.bat

echo.
echo [3/4] Обновление pip и установка/обновление зависимостей...
python -m pip install --upgrade pip
pip install aiogram==2.23.1 matplotlib pandas==2.2.2 sqlalchemy==2.0.19 psycopg2-binary openpyxl

echo.
echo [4/4] Запуск бота...
echo ========================================
echo.

REM Запускаем основной файл бота
python main.py

echo.
echo ========================================
echo Бот завершил работу. Виртуальное окружение все еще активно.
echo Для выхода из окружения введите 'deactivate'
echo ========================================
echo.

pause