# Local Development Setup Guide

This guide will help you set up the Auto Insurance Fraud Detection System on your local machine for development and testing.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.9+** (Python 3.10 or 3.11 recommended)
- **Neo4j Desktop 1.5+** or Neo4j Server 5.x
- **Git** for version control
- **pip** (Python package installer)
- **virtualenv** or **venv** (recommended for isolated environments)

### System Requirements

- **RAM**: Minimum 8GB (16GB recommended)
- **Storage**: 5GB free space
- **OS**: Windows 10/11, macOS 10.15+, or Linux (Ubuntu 20.04+)

---

## Step 1: Clone the Repository

```bash
# Clone the repository
git clone https://github.com/your-org/auto-insurance-fraud-detection.git

# Navigate to project directory
cd auto-insurance-fraud-detection

## Step 2: Set Up Python Environment
### using venv

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate

## Step 3: Install Dependencies
```bash
# Upgrade pip
pip install --upgrade pip

# Install required packages
pip install -r requirements.txt

# Verify installation
pip list

## Step 4: Configure Environment Variables
```bash
### Create a .env file in the project root:

# Copy template
cp .env.example .env

# Edit with your settings
nano .env  # or use your preferred editor


