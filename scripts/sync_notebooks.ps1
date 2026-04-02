jupytext --sync notebooks/*.ipynb `
    --pipe "isort - --treat-comment-as-code '# %%' --float-to-top" `
    --pipe "ruff check - --fix --ignore F821" `
    --pipe "ruff format -"
