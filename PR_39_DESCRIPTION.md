## Description of change

Bumps the tap version from `1.4.1` to `1.5.0` to account for the changes merged in PR #32:

- **OAuth library swap**: Replaced deprecated `oauth2client` with `google-auth` + `google_auth_httplib2` (#33)
- **Centralized error handling**: New `client.py` with backoff/retry for 429 and 5xx errors (#31)
- **Dependency upgrades**: Pinned `singer-python` to 6.4.0, `google-api-python-client` to 2.174.0, added `backoff` 2.2.1, removed `pendulum` and `oauth2client` (#30)

### Why a MINOR version bump (1.5.0) instead of MAJOR (2.0.0)?

Per **semantic versioning** principles, this is classified as a **minor version bump** (backward compatible feature addition) rather than a major version bump for the following reasons:

#### 1. **User-facing interface remains unchanged**
The most important factor: **no configuration changes required**. Users can upgrade without modifying their integration:

**Config format (unchanged):**
```json
{
  "client_id": "...",
  "client_secret": "...",
  "refresh_token": "...",
  "profile_id": "...",
  "user_agent": "..."
}
```

- Command-line usage: identical
- Catalog structure: identical  
- Output schema: identical
- State handling: identical

#### 2. **Dependency changes are implementation details**
While the OAuth library changed internally (`oauth2client` → `google-auth`), this is transparent to users:
- Package managers (pip) automatically handle dependency resolution
- No user action required beyond `pip install --upgrade`
- The tap's **public API contract** (config in, data out) remains unchanged

#### 3. **Improvements are additive, not breaking**
The changes add **new capabilities** without removing existing functionality:
- ✅ Better error handling (retry logic for 429/5xx)
- ✅ More robust authentication (modern, maintained library)
- ✅ Improved reliability (no change to success paths)

#### 4. **SemVer definition of "breaking change"**
A breaking change requires users to **modify their code or configuration** when upgrading. Since:
- ❌ No config file changes needed
- ❌ No catalog modifications needed  
- ❌ No integration code changes needed
- ✅ Drop-in replacement upgrade

This does **not** constitute a breaking change from the user's perspective.

#### 5. **Precedent in tap versioning**
Looking at the CHANGELOG history:
- `1.2.0`, `1.3.0`, `1.4.0` all bumped the API version (v3.3 → v3.5 → v4)
- API version changes affect **external behavior** more significantly than internal library swaps
- If API version bumps are minor, internal library swaps should be too

### Benefits of 1.5.0 over 2.0.0
- **Clearer upgrade path**: Users won't hesitate thinking "what do I need to change?"
- **Accurate signaling**: Version number correctly indicates backward compatibility
- **Encourages adoption**: Minor versions suggest safe, low-risk upgrades

### Changes in this PR
- `setup.py`: version `1.4.1` → `1.5.0`
- `CHANGELOG.md`: Added 1.5.0 release entry with backward compatibility note

---

**Summary**: This is a **feature-enhancement release** with improved error handling and modernized dependencies, not a breaking change requiring user action. Version 1.5.0 accurately reflects this.
