---
kind: fix
date: 2026-10-22
---

* **MongoDB Community**: Generate connection-string Secrets in configured namespaces without invalid cross-namespace owner references. Track their owning resource, preserve unrelated Secrets, and retry failed cleanup before removing the resource finalizer. Handle namespace changes, reused Secret names, and deletion of targets in the current or last successful configuration, including deletion before the first successful reconciliation.
