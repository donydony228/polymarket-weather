#!/bin/bash
# 啟動 Weather Dashboard
cd "$(dirname "$0")"
./venv/bin/streamlit run src/app.py
