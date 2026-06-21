# Releasing LangMonitor

Publishing is automated by [`.github/workflows/publish.yml`](.github/workflows/publish.yml).

## How it works

| Event | What runs |
|-------|-----------|
| **PR opened/updated → `main`** | `test` only (gate on Python 3.10/3.11/3.12). |
| **PR merged → `main`** (push) | `test` → `build` → **publish to PyPI**. |
| **Manual** (`workflow_dispatch`) | Same as a merge. |

The build verifies metadata with `twine check` and asserts the bundled dashboard
(`static/dashboard`) is present in the wheel before publishing.

## One-time setup

1. **Add the PyPI token secret.** Create an API token at
   <https://pypi.org/manage/account/token/> (scope it to the `langmonitor`
   project once it exists). In GitHub:
   `Settings → Secrets and variables → Actions → New repository secret`
   - Name: `PYPI_API_TOKEN`
   - Value: the `pypi-…` token
   (Optionally store it on the `pypi` Environment instead, and add a required
   reviewer there to gate every publish behind a manual approval.)

2. **Commit the prebuilt dashboard.** The wheel ships `static/dashboard/**` via
   `package-data`, but CI only sees committed files. Make sure `static/` is
   committed:
   ```bash
   git add static && git commit -m "Ship prebuilt dashboard UI"
   ```
   If it's missing, the `build` job fails with a clear error instead of
   publishing a UI-less package.

## Cutting a release

PyPI rejects re-uploads of an existing version, so **bump the version** before
merging the release PR:

```toml
# pyproject.toml
version = "0.1.1"
```

Then merge to `main`. The workflow builds and publishes the new version.

> Merges that *don't* bump the version don't fail — `skip-existing: true` means
> the publish step simply skips an already-published version. So you can merge
> freely; a new release only goes out when the version changes.

## Verifying a release

- Watch the run under the repo's **Actions** tab.
- After it's green: `pip install --upgrade langmonitor` and check the version,
  or see <https://pypi.org/project/langmonitor/>.
