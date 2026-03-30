FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

# Pre-install DuckDB extensions
ENV PATH="/app/.venv/bin:$PATH"
RUN python -c "
import duckdb, platform, os, urllib.request

c = duckdb.connect(':memory:')
ver = c.execute('SELECT version()').fetchone()[0]
c.close()

arch = platform.machine()
arch_map = {'x86_64': 'linux_amd64', 'aarch64': 'linux_arm64', 'arm64': 'linux_arm64'}
plat = arch_map.get(arch, 'linux_amd64')

url = f'https://extensions.duckdb.org/v{ver}/{plat}/httpfs.duckdb_extension.gz'
ext_dir = f'/app/duckdb_extensions/v{ver}/{plat}'
os.makedirs(ext_dir, exist_ok=True)

print(f'Downloading {url}')
urllib.request.urlretrieve(url, f'{ext_dir}/httpfs.duckdb_extension.gz')

import gzip, shutil
with gzip.open(f'{ext_dir}/httpfs.duckdb_extension.gz', 'rb') as f_in:
    with open(f'{ext_dir}/httpfs.duckdb_extension', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
os.remove(f'{ext_dir}/httpfs.duckdb_extension.gz')
print('Done')
"

FROM python:3.13-slim

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/duckdb_extensions /app/duckdb_extensions
COPY config.yaml .
COPY entrypoint.py .

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["python", "-m", "entrypoint"]
