# Installation Troubleshooting Guide

## Pandas Build Failure Solutions

If you're getting errors when trying to install pandas, try these solutions in order:

### Solution 1: Use Pre-compiled Wheels (Recommended)
```bash
# Upgrade pip first
pip install --upgrade pip

# Install with pre-compiled wheels (faster, no compilation needed)
pip install --only-binary=all -r requirements.txt
```

### Solution 2: Use Conda Instead of Pip
```bash
# If you have conda/miniconda installed
conda install pandas flask flask-socketio apscheduler eventlet scikit-learn
```

### Solution 3: Install Dependencies Individually
```bash
# Install pandas first (often the most problematic)
pip install "pandas>=1.5.0,<3.0.0"

# Then install the rest
pip install flask==2.3.3
pip install flask-socketio==5.3.6
pip install apscheduler==3.10.4
pip install eventlet==0.33.3
pip install "scikit-learn>=1.2.0,<2.0.0"
```

### Solution 4: System-Specific Fixes

#### Windows:
```bash
# Install Microsoft C++ Build Tools if needed
# Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/

# Or use Windows Subsystem for Linux (WSL)
```

#### macOS:
```bash
# Install Xcode command line tools
xcode-select --install

# Or use Homebrew
brew install python
pip install -r requirements.txt
```

#### Linux (Ubuntu/Debian):
```bash
# Install build dependencies
sudo apt-get update
sudo apt-get install python3-dev python3-pip build-essential

# Install requirements
pip install -r requirements.txt
```

#### Linux (CentOS/RHEL/Fedora):
```bash
# Install build dependencies
sudo yum install python3-devel gcc gcc-c++ make
# or for newer versions:
sudo dnf install python3-devel gcc gcc-c++ make

pip install -r requirements.txt
```

### Solution 5: Use Virtual Environment
```bash
# Create a fresh virtual environment
python -m venv chat_analytics_env

# Activate it
# Windows:
chat_analytics_env\Scripts\activate
# macOS/Linux:
source chat_analytics_env/bin/activate

# Upgrade pip and install
pip install --upgrade pip
pip install -r requirements.txt
```

### Solution 6: Alternative Minimal Installation
If all else fails, you can try installing minimal versions:
```bash
pip install pandas flask flask-socketio
# The app will still work but some analytics features may be limited
```

## After Successful Installation

1. Run the initialization script:
   ```bash
   python init_database.py
   ```

2. Start the server:
   ```bash
   python run_server.py
   ```

## Still Having Issues?

- Check your Python version: `python --version` (should be 3.8+)
- Check your pip version: `pip --version`
- Try creating a new virtual environment
- Consider using Docker if available

## Quick Docker Alternative
If you have Docker installed:
```dockerfile
# Create a simple Dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "run_server.py"]
```