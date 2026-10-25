"""
Pytest configuration for the Distributed Task Queue test suite.

Adds the ``taskk/`` source directory to ``sys.path`` so that all modules
(config, task_queue, worker, …) are importable without installation.
"""
import os
import sys

# Make the parent directory (taskk/) importable from any working directory
_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)
