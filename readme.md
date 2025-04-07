# Shared Libraries

> Python shared libraries for internal use.

## Getting Started

Follow these steps to set up the project locally in **VSCode**.

---

### 1. Clone the Repository

```bash
git clone https://github.com/deal-source/shared-libraries.git
cd shared-libraries
```

---

### 2. Set Up a Virtual Environment

```bash
python -m venv venv
```

Activate the virtual environment:

- **Windows**:
  ```bash
  venv\Scripts\activate
  ```
- **Mac/Linux**:
  ```bash
  source venv/bin/activate
  ```

---

### 3. Install Requirements

```bash
pip install -r requirements.txt
```

---

### 4. Set Up Environment Variable

Set the `OPENAI_API_KEY` environment variable:

- **Windows** (PowerShell):
  ```bash
  $env:OPENAI_API_KEY="your-openai-api-key"
  ```

- **Mac/Linux** (bash/zsh):
  ```bash
  export OPENAI_API_KEY="your-openai-api-key"
  ```

---

### 5. Run the Pipeline

```bash
python -m app.business.pipeline
```

---

## Project Structure

```
shared-libraries/
|
├── app/
│   ├── business/
│   │   └── pipeline.py
│   └── ...
├── venv/ (optional)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Notes

- Ensure you run the `pipeline.py` **as a module** with the `-m` flag to correctly resolve relative imports.
- The project requires an OpenAI API Key for access to OpenAI services.

---

## License

This project is licensed internally. See the repository for more details.

