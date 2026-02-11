# Changelog

## 1.5.0
  * Replace deprecated `oauth2client` with `google-auth` library for OAuth2 authentication [#33](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/33)
  * Add centralized error handling client with backoff/retry for 429 and 5xx errors [#31](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/31)
  * Upgrade dependencies: `singer-python` to 6.4.0, `google-api-python-client` to 2.174.0, add `backoff` 2.2.1 [#30](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/30)
  * **Note**: This is a minor version (not major) as all user-facing configuration remains backward compatible - no config changes required for upgrade

## 1.4.1
  * Explicitly tolerate file status of 'QUEUED' [#27](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/27)

## 1.4.0
  * Bump API version from 3.5 -> 4 [#23](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/23)

## 1.3.0
  * Bump API version from 3.3 -> 3.5 [#20](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/20)

## 1.2.0
  * Bump API version to 3.3 [#11](https://github.com/singer-io/tap-doubleclick-campaign-manager/pull/11)

## 1.1.0
  * Bump API version from 3.1 -> 3.2

## 1.0.0
  * General release
