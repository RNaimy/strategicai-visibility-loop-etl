# Contributing to StrategicAI Visibility Loop ETL

We love contributions from the community! This project is designed to help digital marketers, SEO analysts, and developers merge analytics data into actionable visibility insights — so your improvements make a real difference.

---

## 🧭 How to Get Started

1. **Fork this repository**  
   Click “Fork” at the top-right of the repo and clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/strategicai-visibility-loop-etl.git
   cd strategicai-visibility-loop-etl
   ```

2. **Set up your environment**
   ```bash
   make setup
   ```

3. **Run the demo**
   ```bash
   make run
   ```

4. **Explore outputs**
   The main output file (`merged/merged_visibility.csv`) will appear in the `merged/` folder.

---

## 🛠️ Development Workflow

- **Create a new branch** for each contribution:
  ```bash
  git checkout -b feature/add-new-transform
  ```

- **Make your changes**, keeping code readable and commented.

- **Run checks**:
  ```bash
  make clean && make setup && make run
  ```

- **Commit & push**:
  ```bash
  git add .
  git commit -m "feat: describe your change"
  git push origin feature/add-new-transform
  ```

- **Open a Pull Request** on GitHub.  
  Include screenshots or before/after examples if relevant.

---

## 🧪 Testing Your Changes

Before submitting, make sure:
- Your code runs end-to-end using the demo dataset (`data_demo/`).
- The merged output (`merged/merged_visibility.csv`) is created successfully.
- No sensitive or private URLs (like client data) appear in sample outputs.
- Run `detect-secrets scan` to ensure no secrets are committed.

---

## 📏 Code Style

Follow these simple guidelines:
- Use **PEP8** conventions.
- Keep functions small and modular.
- Prefer **pandas** transformations over loops.
- Add docstrings with purpose, inputs, and outputs.
- Use `print()` sparingly — prefer `logging.info()` for traceable debug info.

---

## 🤝 Code of Conduct

This project follows the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).  
Please treat others with respect, professionalism, and curiosity.

---

## 💡 Ways to Contribute

- Improve README or documentation clarity
- Enhance data merge logic or configuration options
- Add support for new data sources (e.g., Ahrefs, Semrush)
- Fix bugs or optimize performance
- Improve test coverage

---

## 🧬 License

By contributing, you agree that your contributions will be licensed under the **MIT License**, the same as the project.

---

**Thanks for helping improve the StrategicAI Visibility Loop ETL!**  
Your ideas make SEO data analysis easier for everyone.