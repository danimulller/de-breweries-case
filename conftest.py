import sys
import os

# Ensures that 'src' is importable when running pytest from the project root
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
