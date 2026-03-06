# RL-Coach-Agent

A Mastra-based agentic project for Rocket League coaching.

## Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose

## Setup Instructions

1. **Environment Variables**:
   Update the `.env` file with your specific credentials and keys, notably `BALLCHASING_API_KEY`.

2. **Start the Database**:
   Run the following command to start the PostgreSQL 16 database:
   ```bash
   docker compose up -d
   ```

3. **Install Dependencies**:
   This project uses `uv` for dependency management. To sync dependencies and create a virtual environment:
   ```bash
   uv sync
   ```

4. **Project Structure**:
   - `/src/agents`: For Mastra agent definitions.
   - `/src/tools`: For MCP tools and data processing scripts.
   - `/src/database`: For SQL migrations and schemas.
   - `/data/raw`: For storing downloaded JSON replays.
